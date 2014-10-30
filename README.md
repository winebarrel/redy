# Redy

Redy is DynamoDB client to cache data in Redis.
It depends on  [fluent-plugin-dynamodb-alt](https://github.com/winebarrel/fluent-plugin-dynamodb-alt).

## Architecture

### Write
![](https://github.com/winebarrel/redy/raw/master/etc/write.png)

### Read
![](https://github.com/winebarrel/redy/raw/master/etc/read.png)

### Redis down
![](https://github.com/winebarrel/redy/raw/master/etc/redis-down.png)

## Benefit

* High availability. There is no significant impact Redis is down.
* Speed. You can quickly access the object because it is cached in Redis.
* Capacity. large capacity at a cheap price ([$205 / 120GB r:100 w:100 / month](http://calculator.s3.amazonaws.com/index.html))

## Usage

```ruby
redy = Redy.new(
  redis: {namespace: 'redy', host: 'example.com', port: 7777, expire_after: 86400, negative_cache_ttl: 300},
  fluent: {tag: 'dynamodb.test', host: 'localhost', port: 24224, redis_error_tag: 'redis.error'},
  dynamodb: {table_name: 'my_table', timestamp_key: 'timestamp', access_key_id: '...', secret_access_key: '...', region: 'us-east-1', delete_key: 'delete'})

redy.set('foo', {'bar' => 100, 'zoo' => 'baz'}, :async => true)
p redy.get('foo') #=> {'bar' => 100, 'zoo' => 'baz'}
redy.delete('foo')

redy.set('bar', 100, :expire_after => 5)
p redy.get('bar') #=> 100
```

## Fluentd configuration

See [fluent-plugin-dynamodb-alt](https://github.com/winebarrel/fluent-plugin-dynamodb-alt).

```
<match dynamodb.**>
  type dynamodb_alt
  aws_key_id AKIAIOSFODNN7EXAMPLE
  aws_sec_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  region ap-northeast-1
  table_name my_table
  timestamp_key timestamp
  binary_keys data
  delete_key delete
  expected id NULL,timestamp LE ${timestamp}
  conditional_operator OR
  flush_interval 1s
</match>
```

## Test

```sh
brew install redis
npm install -g dynalite
bundle install
bundle exec rake
```

## Limitation

* Use [MessagePack](http://msgpack.org/) to serialize. (You can not save complex objects)
* The maximum recode data size is [64KB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html). (There is improvement plan)
