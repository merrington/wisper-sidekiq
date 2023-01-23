class Subscriber
  def self.it_happened(message, **_kwargs)
    File.open('/tmp/shared', 'w') do |file|
      file.puts "pid: #{Process.pid}"
      file.puts message.inspect
    end
  end
end
