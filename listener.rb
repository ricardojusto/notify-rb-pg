# frozen_string_literal: true

require 'pg'

class Listener
  def initialize
    @conn = create_connection
  end

  def listen
    @conn.wait_for_notify do |_event, _pid, payload|
      puts payload
    end
  end

  def create
    create_function
    create_trigger
    puts 'function and trigger have been created'
  rescue => error
    puts error
  end

  def drop
    drop_trigger
    drop_function
    puts 'function and trigger have been deleted'
  rescue => error
    puts error
  end

  private

  def create_connection
    PG::Connection.open(
      host: 'localhost',
      dbname: '',
      user: '',
      password: '',
      port: 5432,
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

  def create_trigger
    trigger = <<-SQL
      CREATE TRIGGER notify_order_event
      AFTER INSERT OR UPDATE OR DELETE ON orders
        FOR EACH ROW EXECUTE PROCEDURE notify_event();
    SQL
    @conn.exec(trigger)
  end

  def drop_trigger
    @conn.exec('DROP TRIGGER notify_order_event ON orders')
  end
end
