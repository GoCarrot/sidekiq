module Sidekiq
  module Backend
    class Redis
      def atomic_push(conn, payloads)
        if payloads.first['at']
          conn.zadd('schedule'.freeze, payloads.map do |hash|
            at = hash.delete('at'.freeze).to_s
            [at, Sidekiq.dump_json(hash)]
          end)
        else
          q = payloads.first['queue']
          now = Time.now.to_f
          to_push = payloads.map do |entry|
            entry['enqueued_at'.freeze] = now
            Sidekiq.dump_json(entry)
          end
          conn.sadd('queues'.freeze, q)
          conn.lpush("queue:#{q}", to_push)
        end
      end

      def raw_push(payloads, redis_pool)
        redis_pool.with do |conn|
          conn.multi do
            atomic_push(conn, payloads)
          end
        end
        true
      end
    end
  end
end
