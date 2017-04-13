require File.dirname(__FILE__) + '/spec_helper'

describe "redis" do
  before(:all) do
    # use database 15 for testing so we dont accidentally step on your real data
    @redis = Redis.new :db => 15
  end

  before(:each) do
    @redis.flushdb
  end

  after(:all) do
    @redis.quit
  end

  shared_examples_for "a semaphore" do

    it "has the correct amount of available resources" do
      semaphore.lock
      expect(semaphore.unlock).to eq(1)
      expect(semaphore.available_count).to eq(1)
    end

    it "has the correct amount of available resources before locking" do
      expect(semaphore.available_count).to eq(1)
    end

    it "should not exist from the start" do
      expect(semaphore.exists?).to eq(false)
      semaphore.lock
      expect(semaphore.exists?).to eq(true)
    end

    it "should be unlocked from the start" do
      expect(semaphore.locked?).to eq(false)
    end

    it "should lock and unlock" do
      semaphore.lock
      expect(semaphore.locked?).to eq(true)
      semaphore.unlock
      expect(semaphore.locked?).to eq(false)
    end

    it "should not lock twice as a mutex" do
      expect(semaphore.lock).not_to eq(false)
      expect(semaphore.lock).to eq(false)
    end

    it "should not lock three times when only two available" do
      expect(multisem.lock).not_to eq(false)
      expect(multisem.lock).not_to eq(false)
      expect(multisem.lock).to eq(false)
    end

    it "should always have the correct lock-status" do
      multisem.lock
      multisem.lock

      expect(multisem.locked?).to eq(true)
      multisem.unlock
      expect(multisem.locked?).to eq(true)
      multisem.unlock
      expect(multisem.locked?).to eq(false)
    end

    it "should get all different tokens when saturating" do
      ids = []
      2.times do
        ids << multisem.lock
      end

      expect(ids).to eq(%w(0 1))
    end

    it "should execute the given code block" do
      code_executed = false
      semaphore.lock do
        code_executed = true
      end
      expect(code_executed).to eq(true)
    end

    it "should pass an exception right through" do
      expect {
        semaphore.lock do
          raise Exception, "redis semaphore exception"
        end
      }.to raise_error(Exception, "redis semaphore exception")
    end

    it "should not leave the semaphore locked after raising an exception" do
      expect {
        semaphore.lock do
          raise Exception, "redis semaphore exception"
        end
      }.to raise_error(Exception, "redis semaphore exception")

      expect(semaphore.locked?).to eq(false)
    end

    it "should return the value of the block if block-style locking is used" do
      block_value = semaphore.lock do
        42
      end
      expect(block_value).to eq(42)
    end

    it "can return the passed in token to replicate old behaviour" do
      lock_token = semaphore.lock
      semaphore.unlock()

      block_value = semaphore.lock do |token|
        token
      end
      expect(block_value).to eq(lock_token)
    end

    it "should disappear without a trace when calling `delete!`" do
      original_key_size = @redis.keys.count

      semaphore.exists_or_create!
      semaphore.delete!

      expect(@redis.keys.count).to eq(original_key_size)
    end

    it "should not block when the timeout is zero" do
      did_we_get_in = false

      semaphore.lock do
        semaphore.lock(0) do
          did_we_get_in = true
        end
      end

      expect(did_we_get_in).to be false
    end

    it "should be locked when the timeout is zero" do
      semaphore.lock(0) do
        expect(semaphore.locked?).to be true
      end
    end
  end

  describe "semaphore with expiration" do
    let(:semaphore) { Redis::Semaphore.new(:my_semaphore, :redis => @redis, :expiration => 2) }
    let(:multisem) { Redis::Semaphore.new(:my_semaphore_2, :resources => 2, :redis => @redis, :expiration => 2) }

    it_behaves_like "a semaphore"

    it "expires keys" do
      original_key_size = @redis.keys.count
      semaphore.exists_or_create!
      sleep 3.0
      expect(@redis.keys.count).to eq(original_key_size)
    end

    it "expires keys after unlocking" do
      original_key_size = @redis.keys.count
      semaphore.lock do
        # noop
      end
      sleep 3.0
      expect(@redis.keys.count).to eq(original_key_size)
    end
  end

  describe "semaphore with staleness checking" do
    let(:semaphore) { Redis::Semaphore.new(:my_semaphore, :redis => @redis, :stale_client_timeout => 5) }
    let(:multisem) { Redis::Semaphore.new(:my_semaphore_2, :resources => 2, :redis => @redis, :stale_client_timeout => 5) }

    it_behaves_like "a semaphore"

    it "should restore resources of stale clients" do
      hyper_aggressive_sem = Redis::Semaphore.new(:hyper_aggressive_sem, :resources => 1, :redis => @redis, :stale_client_timeout => 2)

      expect(hyper_aggressive_sem.lock).not_to eq(false)
      expect(hyper_aggressive_sem.lock).to eq(false)
      Timecop.travel(Time.now + hyper_aggressive_sem.create_release_interval) do
        expect(hyper_aggressive_sem.lock).not_to eq(false)
      end
    end
  end

  describe "minimize redis queries" do
    it "queries while creating" do
      mock = double()
      expect(mock).to receive("lpop") { nil }

      # create lock
      expect(mock).to receive("getset") { nil }
      expect(mock).to receive("expire") { nil }
      expect(mock).to receive("multi") { true }.exactly(2).times

      expect(mock).to receive("lpop") { "1" }
      expect(mock).to receive("hset")

      semaphore = Redis::Semaphore.new(:test, redis: mock)
      semaphore.lock {}
    end

    it "queries when acquiring semaphore" do
      mock = double()
      expect(mock).to receive("lpop") { "1" }
      expect(mock).to receive("hset")
      expect(mock).to receive("multi")

      semaphore = Redis::Semaphore.new(:test, redis: mock)
      semaphore.lock {}
    end

    it "queries when releasing stale locks" do
      mock = double()
      expect(mock).to receive("lpop") { nil }

      # create
      expect(mock).to receive("getset") { "1" }

      # lock for releasing
      expect(mock).to receive("setnx") { true }
      expect(mock).to receive("del") { true }

      # release stale locks
      expect(mock).to receive("hgetall") { [["0", Time.now - 11]] }

      # retry and get lock
      expect(mock).to receive("lpop") { "0" }
      expect(mock).to receive("hset")

      # release stale lock
      # release own lock
      expect(mock).to receive("multi").exactly(2).times

      semaphore = Redis::Semaphore.new(:test, redis: mock, stale_client_timeout: 10)
      semaphore.lock {}
    end

    it "queries less when it has recently released stale locks" do
      semaphore = Redis::Semaphore.new(:test, redis: @redis, stale_client_timeout: 10)
      semaphore.release_stale_locks!

      mock = double()
      expect(mock).to receive("lpop") { "1" }
      expect(mock).to receive("hset")
      expect(mock).to receive("multi")

      semaphore.redis = mock
      semaphore.lock {}
    end
  end

  # Private method tests, do not use
  describe "simple_expiring_mutex" do
    let(:semaphore) { Redis::Semaphore.new(:my_semaphore, :redis => @redis) }

    before do
      semaphore.class.send(:public, :simple_expiring_mutex)
    end

    it "gracefully expires stale lock" do
      expiration = 1

      thread =
        Thread.new do
          semaphore.simple_expiring_mutex(:test, expiration) do
            sleep 3
          end
        end

      sleep 1.5

      expect(semaphore.simple_expiring_mutex(:test, expiration)).to be_falsy

      sleep expiration

      it_worked = false
      semaphore.simple_expiring_mutex(:test, expiration) do
        it_worked = true
      end

      expect(it_worked).to be_truthy
      thread.join
    end
  end
end
