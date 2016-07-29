#pretty useful rails method.  Splits an array into groups
class Array
  def in_groups(number, fill_with = nil)
    # size / number gives minor group size;
    # size % number gives how many objects need extra accommodation;
    # each group hold either division or division + 1 items.
    division = size / number
    modulo = size % number

    # create a new array avoiding dup
    groups = []
    start = 0

    number.times do |index|
      length = division + (modulo > 0 && modulo > index ? 1 : 0)
      padding = fill_with != false && modulo > 0 && length == division ? 1 : 0
      groups << slice(start, length).concat([fill_with] * padding)
      start += length
    end

    if block_given?
      groups.each { |g| yield(g) }
    else
      groups
    end
  end
end


require 'activerecord-import/base'

class ActiveRecord::Base
  class << self
    # Imports the passed in +column_names+ and +array_of_attributes+
    # given the passed in +options+ Hash. This will return the number
    # of insert operations it took to create these records without
    # validations or callbacks. See ActiveRecord::Base.import for more
    # information on +column_names+, +array_of_attributes_ and
    # +options+.
    def import_without_validations_or_callbacks( column_names, array_of_attributes, options = {} )
      column_names = column_names.map(&:to_sym)
      scope_columns, scope_values = scope_attributes.to_a.transpose

      unless scope_columns.blank?
        scope_columns.zip(scope_values).each do |name, value|
          name_as_sym = name.to_sym
          next if column_names.include?(name_as_sym)

          is_sti = (name_as_sym == inheritance_column.to_sym && self < base_class)
          value = value.first if is_sti

          column_names << name_as_sym
          array_of_attributes.each { |attrs| attrs << value }
        end
      end

      columns = column_names.each_with_index.map do |name, i|
        column = columns_hash[name.to_s]

        raise ActiveRecord::Import::MissingColumnError.new(name.to_s, i) if column.nil?

        column
      end

      columns_sql = "(#{column_names.map { |name| connection.quote_column_name(name) }.join(',')})"
      insert_sql = "INSERT #{options[:ignore] ? 'IGNORE ' : ''}INTO #{quoted_table_name} #{columns_sql} VALUES "
      values_sql = values_sql_for_columns_and_attributes(columns, array_of_attributes)

      unless options[:on_duplicate_key_update].empty?
        duplicate_sql = " ON DUPLICATE KEY UPDATE " << options[:on_duplicate_key_update].map { |it| "#{it}=VALUES(#{it})" }.join(', ')
        values_sql.each { |value| value << duplicate_sql }
      end

      number_inserted = 0
      ids = []
      if supports_import?
        # generate the sql
        post_sql_statements = connection.post_sql_statements( quoted_table_name, options )

        batch_size = options[:batch_size] || values_sql.size
        values_sql.each_slice(batch_size) do |batch_values|
          # perform the inserts
          result = connection.insert_many( [insert_sql, post_sql_statements].flatten,
            batch_values,
            "#{self.class.name} Create Many Without Validations Or Callbacks" )
          number_inserted += result[0]
          ids += result[1]
        end
      else
        values_sql.each do |values|
          ids << connection.insert(insert_sql + values)
          number_inserted += 1
        end
      end
      [number_inserted, ids]
    end
  end
end
