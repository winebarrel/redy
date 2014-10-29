class Redy
  DYNAMO_DB_DATA_KEY = 'data'
  DYNAMO_DB_EXPIRE_AFTER_KEY = 'expire_after'
  REDIS_NULL = ''
  NEGATIVE_CACHE_TTL = 60

  def initialize(options = {})
    @logger = options[:logger]

    init_redis(options[:redis] || {})
    init_fluent(options[:fluent] || {})
    init_dynamodb(options[:dynamodb] || {})
  end

  def get(key, options = {})
    key = key.to_s
    serialized = options[:consistent] ? nil : get_redis(key)

    if serialized == REDIS_NULL
      serialized = nil
    elsif not serialized
      serialized = get_dynamodb(key)
    end

    serialized ? deserialize(serialized) : nil
  rescue => e
    log_error(e)
    nil
  end

  def set(key, value, options = {})
    if options[:delete] and not @dynamodb_delete_key
      raise "'dynamodb.delete_key' has not been set"
    end

    key = key.to_s
    serialized = serialize(value)

    if options[:async]
      begin
        key = key.to_s
        serialized = serialize(value)

        begin
          set_redis(key, serialized, options)
        ensure
          set_fluent(key, serialized, options)
        end

        value
      rescue => e
        log_error(e)
        nil
      end
    else
      set_dynamodb(key, serialized, options)

      begin
        set_redis(key, serialized, {:without_dynamodb => true}.merge(options))
      rescue => e
        log_error(e)
        nil
      end
    end
  end

  def delete(key, options = {})
    set(key, nil, {:delete => true}.merge(options))
  end

  private

  def init_redis(options)
    return if options[:stub]

    namespace = options.delete(:namespace)
    @redis_expire_after = options.delete(:expire_after)

    @redis_negative_cache_ttl = options.delete(:negative_cache_ttl) || NEGATIVE_CACHE_TTL

    redis_conn = Redis.new(options)

    if namespace
      @redis = Redis::Namespace.new(namespace, :redis => redis_conn)
    else
      @redis = redis_conn
    end
  end

  def init_fluent(options)
    return if options[:stub]

    @fluent_tag = options[:tag]
    raise "'fluent.tag' is required: #{options.inspect}" unless @fluent_tag

    @fluent_redis_error_tag = options[:redis_error_tag]

    @fluent = Fluent::Logger::FluentLogger.new(nil, options)
  end

  def init_dynamodb(options)
    return if options[:stub]

    @dynamodb_table_name = options.delete(:table_name)
    raise "'dynamodb.table_name' is required: #{options.inspect}" unless @dynamodb_table_name

    @dynamodb_timestamp_key = options.delete(:timestamp_key)
    raise "'dynamodb.timestamp_key' is required: #{options.inspect}" unless @dynamodb_timestamp_key

    @dynamodb_data_key = options.delete(:data_key) || DYNAMO_DB_DATA_KEY
    @dynamodb_expire_after_key = options.delete(:expire_after_key) || DYNAMO_DB_EXPIRE_AFTER_KEY
    @dynamodb_delete_key = options.delete(:delete_key)

    @dynamodb = Aws::DynamoDB::Client.new(options)
    table = @dynamodb.describe_table(:table_name => @dynamodb_table_name)

    @dynamodb_hash_key = table.table.key_schema.find {|i| i.key_type == 'HASH' }.attribute_name
  end

  def get_redis(key)
    # stub mode
    return nil unless @redis

    @redis.get(key)
  rescue => e
    log_error(e)
    get_dynamodb(key, :without_redis => true)
  end

  def get_dynamodb(key, options = {})
    # stub mode
    return nil unless @dynamodb

    item = @dynamodb.get_item(
      :table_name => @dynamodb_table_name,
      :key => {@dynamodb_hash_key => key}
    ).item

    if item
      data = item[@dynamodb_data_key].string

      if expired?(item)
        set_fluent(key, data, {:delete => true}.merge(options))
        set_negative_cache(key) unless options[:without_redis]
        nil
      else
        set_redis(key, data, :without_dynamodb => true) unless options[:without_redis]
        data
      end
    else
      set_negative_cache(key) unless options[:without_redis]
      nil
    end
  end

  def set_redis(key, serialized, options = {})
    # stub mode
    return unless @redis

    if delete?(options)
      set_negative_cache(key, :raise_error => true)
    elsif (expire_after = min_expire_after(options[:expire_after], @redis_expire_after))
      @redis.setex(key, expire_after, serialized)
    else
      @redis.set(key, serialized)
    end
  rescue => e
    log_error(e)
    set_fluent(key, serialized, {:tag => @fluent_redis_error_tag}.merge(options)) if @fluent_redis_error_tag
    set_dynamodb(key, serialized, options) unless options[:without_dynamodb]
  end

  def set_negative_cache(key, options = {})
    # stub mode
    return unless @redis

    begin
      @redis.setex(key, @redis_negative_cache_ttl, REDIS_NULL) unless @redis_negative_cache_ttl.zero?
    rescue => e
      if options[:raise_error]
        raise e
      else
        log_error(e)
      end
    end
  end

  def set_fluent(key, serialized, options = {})
    # stub mode
    return nil unless @fluent

    tag = options[:tag] || @fluent_tag
    add_delete_key_if(options)
    add_expire_after_if(options)

    @fluent.post(tag, item(key, serialized, options))
  end

  def set_dynamodb(key, serialized, options = {})
    # stub mode
    return nil unless @fluent

    if delete?(options)
      @dynamodb.delete_item(
        :table_name => @dynamodb_table_name,
        :key => {@dynamodb_hash_key => key}
      )
    else
      options = {:convert_set => true}.merge(options)
      add_expire_after_if(options)

      @dynamodb.put_item(
        :table_name => @dynamodb_table_name,
        :item => item(key, StringIO.new(serialized), options))
    end
  end

  def item(key, serialized, options = {})
    item = {
      @dynamodb_hash_key => key,
      @dynamodb_timestamp_key => current_timestamp,
      @dynamodb_data_key => serialized,
    }.merge(options[:extra] || {})

    if options[:convert_set]
      convert_set!(item)
    else
      item
    end
  end

  def serialize(data)
    MessagePack.pack(data)
  end

  def deserialize(serialized)
    MessagePack.unpack(serialized)
  end

  def current_timestamp
    now = Time.now
    ('%d%06d' % [now.tv_sec, now.tv_usec]).to_i
  end

  def format_exception(e)
    (["#{e.class}: #{e.message}"] + e.backtrace).join("\n\tfrom ")
  end

  def convert_set!(record)
    record.each do |key, val|
      if val.kind_of?(Array)
        record[key] = Set.new(val)
      end
    end

    return record
  end

  def log_error(e)
    @logger.warn(format_exception(e)) if @logger
  end

  def delete?(options)
    @dynamodb_delete_key and options[:delete]
  end

  def add_delete_key_if(options)
    if delete?(options)
      options[:extra] ||= {}
      options[:extra][@dynamodb_delete_key] = 1
    end
  end

  def add_expire_after_if(options)
    if options[:expire_after]
      options[:extra] ||= {}
      options[:extra][@dynamodb_expire_after_key] = options[:expire_after]
    end
  end

  def expired?(item)
    if item[@dynamodb_expire_after_key]
      expire_after = item[@dynamodb_expire_after_key]
      timestamp = item[@dynamodb_timestamp_key] / 1000000
      timestamp + expire_after < Time.now.to_i
    else
      false
    end
  end

  def min_expire_after(expire_after1, expire_after2)
    if expire_after1 and expire_after2
      [expire_after1, expire_after2].min
    elsif expire_after1
      expire_after1
    elsif expire_after2
      expire_after2
    else
      nil
    end
  end
end
