describe Redy do
  context 'set to redis/dynamodb and get from redis' do
    subject { redy }

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]
    end
  end

  context 'set to redis/dynamodb and get from redis (with expire_after)' do
    subject do
      redy do |options|
        options[:redis][:expire_after] = 1
      end
    end

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      sleep 5

      expect(redis_select_all).to eq({})

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]
    end
  end

  context 'set to redis/dynamodb and get from dynamodb' do
    subject { redy }

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)
      subject.set('hoge', 'FUGA', :async => true)

      redis_truncate
      expect(redis_select_all).to eq({})

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
        {"id" => "hoge", "timestamp" => TEST_TIMESTAMP, "data" => 'FUGA'},
      ]

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      # cache to redis
      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })
    end
  end

  context 'redis donw' do
    subject do
      redy do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
      end
    end

    before do
      restart_fluentd(:flush_interval => 600)
    end

    after do
      restart_fluentd
    end

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(redis_select_all).to eq({})

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})
    end
  end

  context 'dynamodb down' do
    subject { redy }

    before do
      allow(subject.instance_variable_get(:@fluent)).to receive(:post)
      expect(subject.instance_variable_get(:@dynamodb)).not_to receive(:put_item)
    end

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array []
    end
  end

  context 'with extra' do
    subject { redy }

    it do
      subject.set('foo', 100, :extra => {'extra_key1' => 1, 'extra_key2' => ['2', '3']}, :async => true)
      subject.set('bar', 'STR', :extra => {'extra_key1' => 11, 'extra_key2' => [22, 33]}, :async => true)
      subject.set('zoo', [1, '2', 3], :extra => {'extra_key1' => 111, 'extra_key2' => ['222', '333']}, :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :extra => {'extra_key1' => 1111, 'extra_key2' => [2222, 3333]}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      wait_for_fluentd_flush

      rows = dynamodb_select_all.map do |row|
        row['extra_key2'].sort!
        row
      end

      expect(rows).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100, 'extra_key1' => 1, 'extra_key2' => ['2', '3']},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR', 'extra_key1' => 11, 'extra_key2' => [22, 33]},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3], 'extra_key1' => 111, 'extra_key2' => ['222', '333']},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}, 'extra_key1' => 1111, 'extra_key2' => [2222, 3333]},
      ]
    end
  end

  context 'with extra (redis donw)' do
    subject do
      redy do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
      end
    end

    before do
      restart_fluentd(:flush_interval => 600)
    end

    after do
      restart_fluentd
    end

    it do
      subject.set('foo', 100, :extra => {'extra_key1' => 1, 'extra_key2' => ['2', '3']}, :async => true)
      subject.set('bar', 'STR', :extra => {'extra_key1' => 11, 'extra_key2' => [22, 33]}, :async => true)
      subject.set('zoo', [1, '2', 3], :extra => {'extra_key1' => 111, 'extra_key2' => ['222', '333']}, :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :extra => {'extra_key1' => 1111, 'extra_key2' => [2222, 3333]}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({})

      rows = dynamodb_select_all.map do |row|
        row['extra_key2'].sort!
        row
      end

      expect(rows).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100, 'extra_key1' => 1, 'extra_key2' => ['2', '3']},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR', 'extra_key1' => 11, 'extra_key2' => [22, 33]},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3], 'extra_key1' => 111, 'extra_key2' => ['222', '333']},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}, 'extra_key1' => 1111, 'extra_key2' => [2222, 3333]},
      ]
    end
  end

  context 'redis donw with error_tag' do
    subject do
      redy do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
        options[:fluent][:redis_error_tag] = 'redis.error'
      end
    end

    before do
      restart_fluentd(:flush_interval => 600)

      fluent = subject.instance_variable_get(:@fluent)
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"foo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack(100)})
      expect(fluent).to receive(:post).with("redis.error",   {"id"=>"foo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack(100)})
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"bar", "timestamp"=>1409839004901404, "data"=>MessagePack.pack("STR")})
      expect(fluent).to receive(:post).with("redis.error",   {"id"=>"bar", "timestamp"=>1409839004901404, "data"=>MessagePack.pack("STR")})
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"zoo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack([1, '2', 3])})
      expect(fluent).to receive(:post).with("redis.error",   {"id"=>"zoo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack([1, '2', 3])})
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"baz", "timestamp"=>1409839004901404, "data"=>MessagePack.pack({'num' => 200, 'str' => 'XXX'})})
      expect(fluent).to receive(:post).with("redis.error",   {"id"=>"baz", "timestamp"=>1409839004901404, "data"=>MessagePack.pack({'num' => 200, 'str' => 'XXX'})})
    end

    after do
      restart_fluentd
    end

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(redis_select_all).to eq({})

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})
    end
  end

  context 'redis up with error_tag' do
    subject do
      redy do |options|
        options[:fluent][:redis_error_tag] = 'redis.error'
      end
    end

    before do
      fluent = subject.instance_variable_get(:@fluent)
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"foo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack(100)})
      expect(fluent).not_to receive(:post).with("redis.error", {"id"=>"foo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack(100)})
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"bar", "timestamp"=>1409839004901404, "data"=>MessagePack.pack("STR")})
      expect(fluent).not_to receive(:post).with("redis.error", {"id"=>"bar", "timestamp"=>1409839004901404, "data"=>MessagePack.pack("STR")})
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"zoo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack([1, '2', 3])})
      expect(fluent).not_to receive(:post).with("redis.error", {"id"=>"zoo", "timestamp"=>1409839004901404, "data"=>MessagePack.pack([1, '2', 3])})
      expect(fluent).to receive(:post).with("dynamodb.test", {"id"=>"baz", "timestamp"=>1409839004901404, "data"=>MessagePack.pack({'num' => 200, 'str' => 'XXX'})})
      expect(fluent).not_to receive(:post).with("redis.error", {"id"=>"baz", "timestamp"=>1409839004901404, "data"=>MessagePack.pack({'num' => 200, 'str' => 'XXX'})})
    end

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })
    end
  end

  context 'set negative cache' do
    subject { redy }

    before do
      fluent = subject.instance_variable_get(:@fluent)
      allow(fluent).to receive(:post)
    end

    it do
      expect(subject.get('foo')).to be_nil

      expect(redis_select_all).to eq({
        "redy:foo" => '',
      })

      expect(subject).not_to receive(:get_dynamodb)

      expect(subject.get('foo')).to be_nil
    end
  end

  context 'disable negative cache' do
    subject do
      redy do |options|
        options[:redis][:negative_cache_ttl] = 0
      end
    end

    before do
      fluent = subject.instance_variable_get(:@fluent)
      allow(fluent).to receive(:post)
    end

    it do
      expect(subject.get('foo')).to be_nil
      expect(redis_select_all).to eq({})
    end
  end

  context 'stub redis' do
    subject do
      redy do |options|
        options[:redis].update(
          :stub => true,
          :host => '240.0.0.0',
          :timeout => 0.1)
      end
    end

    before do
      expect(subject.instance_variable_get(:@logger)).not_to receive(:warn)
    end

    it do
      subject.set('foo', 100, :async => true)
      expect(subject.get('foo')).to be_nil

      expect(redis_select_all).to eq({})

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id"=>"foo", "timestamp"=>1409839004901404, "data"=>100}
      ]
    end
  end

  context 'stub dynamodb' do
    subject do
      redy do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
        options[:dynamodb][:stub] = true
      end
    end

    before do
      restart_fluentd(:flush_interval => 600)
    end

    after do
      restart_fluentd
    end

    it do
      subject.set('foo', 100, :async => true)
      expect(subject.get('foo')).to be_nil

      expect(redis_select_all).to eq({})

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array []
    end
  end

  context 'stub fluentd' do
    subject do
      redy do |options|
        options[:fluent][:stub] = true
      end
    end

    before do
      expect(subject.instance_variable_get(:@logger)).not_to receive(:warn)
    end

    it do
      subject.set('foo', 100, :async => true)
      expect(subject.get('foo')).to eq 100

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
      })

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array []
    end
  end

  context 'delete (1)' do
    subject { redy }

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]

      subject.set('foo', nil, :delete => true, :async => true)

      wait_for_fluentd_flush

      expect(redis_select_all).to eq({
        "redy:foo" => '',
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      expect(subject.get('foo')).to be_nil
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(dynamodb_select_all).to match_array [
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]
    end
  end

  context 'delete (2)' do
    subject { redy }

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]

      subject.delete('foo', :async => true)

      wait_for_fluentd_flush

      expect(redis_select_all).to eq({
        "redy:foo" => '',
        "redy:bar" => 'STR',
        "redy:zoo" => [1, '2', 3],
        "redy:baz" => {'num' => 200, 'str' => 'XXX'},
      })

      expect(subject.get('foo')).to be_nil
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(dynamodb_select_all).to match_array [
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]
    end
  end

  context 'delete (redis down)' do
    subject do
      redy do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
      end
    end

    before do
      restart_fluentd(:flush_interval => 600)
    end

    after do
      restart_fluentd
    end

    it do
      subject.set('foo', 100, :async => true)
      subject.set('bar', 'STR', :async => true)
      subject.set('zoo', [1, '2', 3], :async => true)
      subject.set('baz', {'num' => 200, 'str' => 'XXX'}, :async => true)

      expect(subject.get('foo')).to eq 100
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(redis_select_all).to eq({})

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]

      subject.delete('foo', :async => true)

      expect(redis_select_all).to eq({})

      expect(subject.get('foo')).to be_nil
      expect(subject.get('bar')).to eq 'STR'
      expect(subject.get('zoo')).to eq [1, '2', 3]
      expect(subject.get('baz')).to eq({'num' => 200, 'str' => 'XXX'})

      expect(dynamodb_select_all).to match_array [
        {"id" => "bar", "timestamp" => TEST_TIMESTAMP, "data" => 'STR'},
        {"id" => "zoo", "timestamp" => TEST_TIMESTAMP, "data" => [1, '2', 3]},
        {"id" => "baz", "timestamp" => TEST_TIMESTAMP, "data" => {'num' => 200, 'str' => 'XXX'}},
      ]
    end
  end

  context 'expire_after' do
    subject { redy(:disable_timestamp_stub => true) }

    it do
      subject.set('foo', 100, :expire_after => 5, :async => true)

      expect(subject.get('foo')).to eq 100

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
      })

      wait_for_fluentd_flush

      rows = dynamodb_select_all.map do |row|
        row.delete('timestamp')
        row
      end

      expect(rows).to match_array [
        {"id" => "foo", "data" => 100, "expire_after" => 5},
      ]

      sleep 7

      expect(redis_select_all).to eq({})

      expect(subject.get('foo')).to be_nil

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array []
    end
  end

  context 'expire_after (redis down)' do
    subject do
      redy(:disable_timestamp_stub => true) do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
      end
    end

    before do
      restart_fluentd(:flush_interval => 10)
    end

    after do
      restart_fluentd
    end

    it do
      subject.set('foo', 100, :expire_after => 5, :async => true)

      expect(subject.get('foo')).to eq 100

      expect(redis_select_all).to eq({})

      rows = dynamodb_select_all.map do |row|
        row.delete('timestamp')
        row
      end

      expect(rows).to match_array [
        {"id" => "foo", "data" => 100, "expire_after" => 5},
      ]

      sleep 7

      expect(subject.get('foo')).to be_nil

      expect(redis_select_all).to eq({})

      sleep 10

      expect(dynamodb_select_all).to match_array []
    end
  end

  context 'delete error' do
    subject do
      redy do |options|
        options[:dynamodb][:delete_key] = nil
      end
    end

    it do
      subject.set('foo', 100, :expire_after => 5, :async => true)
      expect(subject.get('foo')).to eq 100

      expect {
        expect(subject.delete('foo', :async => true))
      }.to raise_error("'dynamodb.delete_key' has not been set")
    end
  end

  context 'consistent get' do
    subject { redy }

    it do
      subject.set('foo', 100, :async => true)
      redis.set('redy:foo', MessagePack.pack(200))
      expect(redis_select_all).to eq({
        "redy:foo" => 200,
      })

      wait_for_fluentd_flush

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
      ]

      expect(subject.get('foo')).to eq 200
      expect(subject.get('foo', :consistent => true)).to eq 100
      expect(subject.get('foo')).to eq 100
    end
  end

  context 'sync set' do
    subject { redy }

    before do
      expect(subject.instance_variable_get(:@fluent)).not_to receive(:post)
    end

    it do
      subject.set('foo', 100)
      expect(subject.get('foo')).to eq 100

      expect(redis_select_all).to eq({
        "redy:foo" => 100,
      })

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
      ]

      subject.delete('foo')

      expect(redis_select_all).to eq({
        "redy:foo" => '',
      })

      expect(subject.get('foo')).to be_nil

      expect(dynamodb_select_all).to match_array []
    end
  end

  context 'sync set (redis down)' do
    subject do
      redy do |options|
        options[:logger] = nil
        options[:redis].update(
          :host => '240.0.0.0',
          :timeout => 0.1)
      end
    end

    before do
      expect(subject.instance_variable_get(:@fluent)).not_to receive(:post)
    end

    it do
      subject.set('foo', 100)

      expect(redis_select_all).to eq({})

      expect(subject.get('foo')).to eq 100

      expect(dynamodb_select_all).to match_array [
        {"id" => "foo", "timestamp" => TEST_TIMESTAMP, "data" => 100},
      ]

      subject.delete('foo')

      expect(redis_select_all).to eq({})

      expect(subject.get('foo')).to be_nil

      expect(dynamodb_select_all).to match_array []
    end
  end
end
