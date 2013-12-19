module Sidekiq
  module Backend
    class Redis
      def raw_push(payloads)
        pushed = false
        Sidekiq.redis do |conn|
          if payloads.first['at']
            pushed = conn.zadd('schedule', payloads.map do |hash|
              at = hash.delete('at').to_s
              [at, Sidekiq.dump_json(hash)]
            end)
          else
            q = payloads.first['queue']
            to_push = payloads.map { |entry| Sidekiq.dump_json(entry) }
            _, pushed = conn.multi do
              conn.sadd('queues', q)
              conn.lpush("queue:#{q}", to_push)
            end
          end
        end
        pushed
      end
    end
  end
end
