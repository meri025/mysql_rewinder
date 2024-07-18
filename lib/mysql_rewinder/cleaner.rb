require_relative 'cleaner/adapter'
require_relative 'cleaner/mysql2_adapter'
require_relative 'cleaner/trilogy_adapter'

class MysqlRewinder
  class Cleaner
    attr_reader :db_config

    def initialize(db_config, except_tables:, adapter:, logger: nil)
      @db_config = db_config
      @client = Adapter.generate(adapter, db_config.transform_keys(&:to_sym))
      @except_tables = except_tables
      @logger = logger
    end

    def clean_all
      clean(tables: all_tables)
    end

    def clean(tables:)
      target_tables = (tables - @except_tables) & all_tables

      if target_tables.empty?
        @logger&.debug { "[MysqlRewinder][#{@db_config[:database]}] Skip DELETE query because target_table is empty. target tables: #{tables}, except tables: #{@except_tables}, all tables: #{all_tables}." }
        return
      end

      log_and_execute("SET FOREIGN_KEY_CHECKS = 0;")
      log_and_execute(target_tables.map { |table| "DELETE FROM #{table}" }.join(';'))
    end

    def all_tables
      @all_tables ||= @client.query(<<~SQL).flatten
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
      SQL
    end

    private

    def log_and_execute(sql)
      return @client.execute(sql) unless @logger&.debug?

      start_ts = Time.now
      res = @client.execute(sql)
      duration = (Time.now - start_ts) * 1000

      name = "[MysqlRewinder][#{@db_config[:database]}] Cleaner SQL (#{duration.round(1)}ms)"
      msg = "\e[1m\e[30m#{name}\e[0m  \e[34m#{sql}\e[0m"
      @logger.debug msg
      res
    end
  end
end
