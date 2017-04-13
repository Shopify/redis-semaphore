require 'redis'

class Redis
  class Semaphore
    EXISTS_TOKEN = "1"
    API_VERSION = "1"
    DEFAULT_CREATE_RELEASE_INTERVAL = 30

    attr_accessor :redis, :create_release_interval

    # stale_client_timeout is the threshold of time before we assume
    # that something has gone terribly wrong with a client and we
    # invalidate it's lock.
    # Default is nil for which we don't check for stale clients
    # Redis::Semaphore.new(:my_semaphore, :stale_client_timeout => 30, :redis => myRedis)
    # Redis::Semaphore.new(:my_semaphore, :redis => myRedis)
    # Redis::Semaphore.new(:my_semaphore, :resources => 1, :redis => myRedis)
    # Redis::Semaphore.new(:my_semaphore, :host => "", :port => "")
    # Redis::Semaphore.new(:my_semaphore, :path => "bla")
    def initialize(name, opts = {})
      @name = name
      @expiration = opts.delete(:expiration)
      @resource_count = opts.delete(:resources) || 1
      @stale_client_timeout = opts.delete(:stale_client_timeout)
      @create_release_interval = opts.delete(:create_release_interval) || DEFAULT_CREATE_RELEASE_INTERVAL
      @redis = opts.delete(:redis)
      @tokens = []
    end

    def exists_or_create!
      token = @redis.getset(exists_key, EXISTS_TOKEN)
      return true if token
      create!
    end

    def available_count
      if exists?
        @redis.llen(available_key)
      else
        @resource_count
      end
    end

    def delete!
      @redis.del(available_key)
      @redis.del(grabbed_key)
      @redis.del(exists_key)
      @redis.del(version_key)
    end

    def lock(timeout = 0, should_retry = true, &block)
      if timeout.nil? || timeout > 0
        # passing timeout 0 to blpop causes it to block
        _key, current_token = @redis.blpop(available_key, timeout || 0)
      else
        current_token = @redis.lpop(available_key)
      end

      unless current_token
        every('create_and_release', @create_release_interval) do
          exists_or_create!
          release_stale_locks! if check_staleness?
          return lock(timeout, false, &block) if should_retry
        end

        return false
      end

      @tokens.push(current_token)
      @redis.hset(grabbed_key, current_token, current_time.to_f)
      return_value = current_token

      if block_given?
        begin
          return_value = yield current_token
        ensure
          signal(current_token)
        end
      end

      return_value
    end
    alias_method :wait, :lock

    def unlock
      return false unless locked?
      signal(@tokens.pop)[1]
    end

    def locked?(token = nil)
      if token
        @redis.hexists(grabbed_key, token)
      else
        @tokens.each do |token|
          return true if locked?(token)
        end

        false
      end
    end

    def signal(token)
      @redis.multi do
        @redis.hdel grabbed_key, token
        @redis.lpush available_key, token

        set_expiration_if_necessary
      end
    end

    def exists?
      @redis.exists(exists_key)
    end

    def release_stale_locks!
      simple_expiring_mutex(:release_locks, @stale_client_timeout) do
        @redis.hgetall(grabbed_key).each do |token, locked_at|
          timed_out_at = locked_at.to_f + @stale_client_timeout

          if timed_out_at < current_time.to_f
            signal(token)
          end
        end
      end
    end

  private

    def simple_expiring_mutex(key_name, expires_in)
      # Using the locking mechanism as described in
      # http://redis.io/commands/setnx

      key_name = namespaced_key(key_name)
      cached_current_time = current_time.to_f
      my_lock_expires_at = cached_current_time + expires_in + 1

      got_lock = @redis.setnx(key_name, my_lock_expires_at)

      if !got_lock
        # Check if expired
        other_lock_expires_at = @redis.get(key_name).to_f

        if other_lock_expires_at < cached_current_time
          old_expires_at = @redis.getset(key_name, my_lock_expires_at).to_f

          # Check if another client started cleanup yet. If not,
          # then we now have the lock.
          got_lock = (old_expires_at == other_lock_expires_at)
        end
      end

      return false if !got_lock

      begin
        yield
      ensure
        # Make sure not to delete the lock in case someone else already expired
        # our lock, with one second in between to account for some lag.
        @redis.del(key_name) if my_lock_expires_at > (current_time.to_f - 1)
      end
    end

    def create!
      @redis.expire(exists_key, 10)

      @redis.multi do
        @redis.del(grabbed_key)
        @redis.del(available_key)
        @resource_count.times do |index|
          @redis.rpush(available_key, index)
        end
        @redis.set(version_key, API_VERSION)
        @redis.persist(exists_key)

        set_expiration_if_necessary
      end
    end

    def set_expiration_if_necessary
      if @expiration
        [available_key, exists_key, version_key].each do |key|
          @redis.expire(key, @expiration)
        end
      end
    end

    def check_staleness?
      !@stale_client_timeout.nil?
    end

    def namespaced_key(variable)
      "SEMAPHORE:#{@name}:#{variable}"
    end

    def available_key
      @available_key ||= namespaced_key('AVAILABLE')
    end

    def exists_key
      @exists_key ||= namespaced_key('EXISTS')
    end

    def grabbed_key
      @grabbed_key ||= namespaced_key('GRABBED')
    end

    def version_key
      @version_key ||= namespaced_key('VERSION')
    end

    def every(key, timeout)
      @every ||= {}
      if !@every[key] || current_time >= @every[key] + timeout
        @every[key] = Time.now
        yield
      end
    end

    def current_time
      Time.now
    end
  end
end
