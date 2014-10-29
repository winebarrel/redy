require 'redy'
require 'json'
require 'securerandom'
require 'base64'

TEST_TABLE_NAME = 'my_table-' + SecureRandom.uuid
TEST_TIMESTAMP = 1409839004901404
TEST_REDIS_PORT = 57777
TEST_SECONDARY_REDIS_PORT = 57778

def fluentd_conf(options = {})
  options = {
    :type => 'dynamodb_alt',
    :endpoint => 'http://localhost:4567',
    :table_name => TEST_TABLE_NAME,
    :timestamp_key => 'timestamp',
    :binary_keys => 'data',
    :delete_key => 'delete',
    :buffer_type => 'memory',
    :flush_interval => 0,
  }.merge(options)

  options = options.select {|k, v| v }.map {|k, v| "#{k} #{v}" }.join("\n")

  <<-EOS
<source>
  type forward
</source>

<match dynamodb.**>
  #{options}
</match>
  EOS
end

$pids = []

def start_processs(cmd, signal = 'KILL')
  pid = spawn(cmd)
  $pids << [signal, pid]
  pid
end

def kill_all
  $pids.each do |signal, pid|
    Process.kill signal, pid
  end
end

def start_redis(port = TEST_REDIS_PORT)
  start_processs("redis-server --port #{port} --loglevel warning")
end

def redis(port = TEST_REDIS_PORT)
  Redis.new(:port => port)
end

def redis_truncate(port = TEST_REDIS_PORT)
  redis(port).keys.each do |key|
    redis.del(key)
  end
end

def redis_select_all(port = TEST_REDIS_PORT)
  h = {}

  redis(port).keys.each do |key|
    val = redis.get(key)
    h[key] = (val || '').empty? ? val : MessagePack.unpack(redis.get(key))
  end

  h
end

def start_dynalite
  start_processs('dynalite')
end

def start_fluentd(options = {})
  $fluentd_pid = start_processs("fluentd -qq -c /dev/null -i '#{fluentd_conf(options)}'", 'INT')
  sleep 3
end

def restart_fluentd(options = {})
  if $fluentd_pid
    Process.kill 'INT', $fluentd_pid
    $fluentd_pid = nil
  end

  sleep 1

  start_fluentd(options)
end

def wait_for_fluentd_flush
  sleep 1
end

def dynamodb_query(sql)
  out = `ddbcli --url localhost:4567 -e "#{sql.gsub(/\n/, ' ')}"`
  raise out unless $?.success?
  return out
end

def dynamodb_create_table(attrs)
  dynamodb_query("CREATE TABLE #{TEST_TABLE_NAME} #{attrs}")
end

def dynamdb_truncate_table
  dynamodb_query("DELETE ALL FROM #{TEST_TABLE_NAME}")
end

def dynamodb_select_all
  JSON.parse(dynamodb_query("SELECT ALL * FROM #{TEST_TABLE_NAME}")).map do |row|
    data = row['data']
    data = Base64.strict_decode64(data)
    row['data'] = MessagePack.unpack(data)
    row
  end
end

def redy(options = {})
  options = {
    :logger => Logger.new($stderr),
    :redis => {:namespace => 'redy', :port => TEST_REDIS_PORT},
    :fluent => {:tag => 'dynamodb.test'},
    :dynamodb => {:table_name => TEST_TABLE_NAME, :timestamp_key => 'timestamp' , :endpoint => 'http://localhost:4567', :delete_key => 'delete'},
  }.merge(options)

  yield(options) if block_given?

  redy = Redy.new(options)

  unless options[:disable_timestamp_stub]
    allow(redy).to receive(:current_timestamp) { TEST_TIMESTAMP }
  end

  redy
end

RSpec.configure do |config|
  config.before(:all) do
    start_redis
    start_dynalite
    dynamodb_create_table('(id STRING HASH) READ = 20 WRITE = 20')
    start_fluentd
  end

  config.before(:each) do
    redis_truncate
    dynamdb_truncate_table
  end

  config.after(:all) do
    kill_all
  end
end
