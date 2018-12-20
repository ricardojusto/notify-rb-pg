# frozen_string_literal: true

require 'pg'
require 'json'
require 'uri'
require 'net/http'
require 'pry'

class Webhook
  class << self
    def send_notification(payload)
      uri = URI.parse(database_conn(payload))
      request = request_settings(uri)
      http_send(uri, request)
    end

    private

    def database_conn(notification)
      "#{ENV['WEBHOOK_URL']}?notification=#{notification}"
    end

    def http_send(uri, request)
      response = Net::HTTP.start(uri.hostname, uri.port, request_options(uri)) do |http|
        http.request(request)
      end
      response
    end

    def request_settings(uri)
      request = Net::HTTP::Post.new(uri)
      request
    end

    def request_options(uri)
      {
        use_ssl: uri.scheme == 'https'
      }
    end
  end
end

class Listener
  def initialize
    @conn = create_connection
  end

  def listen
    channels.each { |channel| @conn.exec("LISTEN #{channel}") }
    loop do
      @conn.wait_for_notify do |channel, pid, payload|
        puts "Received a NOTIFY on channel #{channel}"
        puts "from PG backend #{pid}"
        puts "saying #{JSON.parse(payload).reject { |k,_| k == 'data' }}"
        Webhook.send_notification(payload)
      end
    end
  ensure
    channels.each { |channel| @conn.exec("UNLISTEN #{channel}") }
    puts 'unlistened'
  end

  def create
    create_function
    create_triggers
    puts 'function and triggers have been created'
  rescue StandardError => error
    puts error
  end

  def drop
    drop_triggers
    drop_function
    puts 'function and triggers have been deleted'
  rescue StandardError => error
    puts error
  end

  private

  def channels
    ['events']
  end

  def tables
    ['orders']
  end

  def create_connection
    PG::Connection.open(
      host: ENV['PG_HOST'],
      dbname: ENV['PG_DBNAME'],
      user: ENV['PG_USER'],
      password: ENV['PG_PASSWORD'],
      port: ENV['PG_PORT']
    )
  end

  def create_function
    function = <<-SQL
      CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
      DECLARE
        record RECORD;
        payload JSON;
      BEGIN
        IF (TG_OP = 'DELETE') THEN
          record = OLD;
        ELSE
          record = NEW;
        END IF;
        payload = json_build_object('table', TG_TABLE_NAME,
                                    'action', TG_OP,
                                    'data', row_to_json(record));
        PERFORM pg_notify('events', payload::text);
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql;
    SQL
    @conn.exec(function)
  end

  def drop_function
    @conn.exec('DROP FUNCTION notify_event();')
  end

  def create_triggers
    tables.each do |table|
      trigger = <<-SQL
        CREATE TRIGGER notify_#{table}_events
        AFTER INSERT OR UPDATE OR DELETE ON #{table}
          FOR EACH ROW EXECUTE PROCEDURE notify_event();
      SQL
      @conn.exec(trigger)
    end
  end

  def drop_triggers
    tables.each do |table|
      @conn.exec("DROP TRIGGER notify_#{table}_events ON #{table}")
    end
  end
end
listener = Listener.new
listener.drop
listener.create
listener.listen
