require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/relation.rb'
require 'active_record/persistence.rb'
require 'active_record/relation/query_methods.rb'

#
# Patching {ActiveRecord} to allow specifying the table name as a function of
# attributes.
#
module ActiveRecord
  #
  # Patches for Persistence to allow certain partitioning (that related to the primary key) to work.
  #
  module Persistence

    def _insert_record(values) # :nodoc:
      primary_key_value = nil

      primary_key = self.class.primary_key
      if primary_key && Hash === values
        primary_key_value = values[primary_key]

        if !primary_key_value && self.class.respond_to?(:prefetch_primary_key?) && self.class.prefetch_primary_key?
          primary_key_value = next_sequence_value
          values[primary_key] = primary_key_value
        end
      end

      # ****** BEGIN PARTITIONED PATCH ******
      actual_arel_table = self.class.dynamic_arel_table(Hash[*values.map{|k,v| [k,v]}.flatten]) if self.class.respond_to?(:dynamic_arel_table)
      arel_table = actual_arel_table ? actual_arel_table : self.class.arel_table
      # ****** END PARTITIONED PATCH ******

      if values.empty?
        im = arel_table.compile_insert(self.class.  connection.empty_insert_statement_value)
      else
        im = arel_table.compile_insert(_substitute_values(values))
      end
      im.into arel_table

      self.class.connection.insert(im, "#{self} Create", primary_key || false, primary_key_value)
    end

    def _delete_record(constraints) # :nodoc:
      constraints = _substitute_values(constraints).map { |attr, bind| attr.eq(bind) }

      # ****** BEGIN PARTITIONED PATCH ******
      actual_arel_table = self.class.dynamic_arel_table(Hash[*constraints.map{|k,v| [k,v]}.flatten]) if self.class.respond_to?(:dynamic_arel_table)
      arel_table = actual_arel_table ? actual_arel_table : self.class.arel_table
      # ****** END PARTITIONED PATCH ******

      dm = Arel::DeleteManager.new
      dm.from(arel_table)
      dm.wheres = constraints

      self.class.connection.delete(dm, "#{self} Destroy")
    end

    # This method is patched to prefetch the primary key (if necessary) and to ensure
    # that the partitioning attributes are always included (AR will exclude them
    # if the db column's default value is the same as the new record's value).
    def _create_record(attribute_names = self.attribute_names)
      # ****** BEGIN PARTITIONED PATCH ******
      if self.id.nil? && self.class.respond_to?(:prefetch_primary_key?) && self.class.prefetch_primary_key?
        self.id = self.class.connection.next_sequence_value(self.class.sequence_name)
        attribute_names |= ["id"]
      end

      if self.class.respond_to?(:partition_keys)
        attribute_names |= self.class.partition_keys.map(&:to_s)
      end
      # ****** END PARTITIONED PATCH ******

      attribute_names &= self.class.column_names
      attributes_values = attributes_with_values_for_create(attribute_names)

      new_id = _insert_record(attributes_values)
      self.id ||= new_id if self.class.primary_key

      @new_record = false

      yield(self) if block_given?

      id
    end

    private
    def _substitute_values(values)
      values.map do |name, value|
        attr = self.class.arel_attribute(name)
        bind = self.class.predicate_builder.build_bind_attribute(name, value)
        [attr, bind]
      end
    end
  end # module Persistence

  module QueryMethods

    # This method is patched to change the default behavior of select
    # to use the Relation's Arel::Table
    def build_select(arel)
      if !select_values.empty?
        expanded_select = select_values.map do |field|
          columns_hash.key?(field.to_s) ? arel_table[field] : field
        end
        arel.project(*expanded_select)
      else
        # ****** BEGIN PARTITIONED PATCH ******
        # Original line:
        # arel.project(@klass.arel_table[Arel.star])
        arel.project(table[Arel.star])
        # ****** END PARTITIONED PATCH ******
      end
    end

  end # module QueryMethods
end # module ActiveRecord
