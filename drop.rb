require 'active_record'
require 'pg'

conn = ActiveRecord::Base.establish_connection(
  adapter:    'postgresql',
  host:       'localhost',
  database:   '',
  username:   '',
  password:   '',
  port:       5432,
)

drop_function = <<-SQL
  DROP FUNCTION notify_event();
SQL

drop_trigger = <<-SQL
  DROP TRIGGER notify_order_event;
SQL

conn.execute(drop_function)
conn.execute(drop_trigger)

puts 'all done..'
