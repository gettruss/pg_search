# frozen_string_literal: true

require "digest"

module PgSearch
  class Configuration
    class Association
      attr_reader :columns

      def initialize(model, name, column_names)
        @model = model
        @name = name
        @columns = Array(column_names).map do |column_name, weight|
          ForeignColumn.new(column_name, weight, @model, self)
        end
      end

      def table_name
        @model.reflect_on_association(@name).table_name
      end

      def join(primary_key, &block)
        "LEFT OUTER JOIN (#{relation(primary_key, &block).to_sql}) #{subselect_alias} ON #{subselect_alias}.id = #{primary_key}"
      end

      def subselect_alias
        Configuration.alias(table_name, @name, "subselect")
      end

      private

      def selects
        if singular_association?
          selects_for_singular_association
        else
          selects_for_multiple_association
        end
      end

      def selects_for_singular_association
        columns.map do |column|
          "#{column.full_name}::text AS #{column.alias}"
        end.join(", ")
      end

      def selects_for_multiple_association
        columns.map do |column|
          "string_agg(#{column.full_name}::text, ' ') AS #{column.alias}"
        end.join(", ")
      end

      def relation(primary_key, &block)
        result = @model.unscoped.joins(@name).select("#{primary_key} AS id, #{selects}")
        result = result.group(primary_key) unless singular_association?

        # Apply optional tenant scoping block to associated relation
        # Example: { |rel| rel.where(tenant_id: current_tenant.id) }
        if block_given?
          begin
            original_where_count = count_where_conditions(result)
            result = block.call(result)
            new_where_count = count_where_conditions(result)

            # Verify tenant scoping actually added filtering to association, if a block was added we should infact see extra clauses chained
            if new_where_count <= original_where_count
              raise SecurityError, "Association tenant scoping must add WHERE conditions for #{@name}"
            end
          rescue SecurityError
            raise # Re-raise security errors
          rescue => e
            # Never allow association queries without tenant isolation when explicit block is passed down
            raise SecurityError, "Association tenant scoping failed for #{@name}: #{e.message}"
          end
        end

        result
      end

      def singular_association?
        %i[has_one belongs_to].include?(@model.reflect_on_association(@name).macro)
      end

      def count_where_conditions(scope)
        # Handle test mocks and edge cases where where_clause might not be available
        return 0 unless scope.respond_to?(:where_clause)

        where_clause = scope.where_clause
        return 0 if where_clause.nil?

        ast = where_clause.ast
        return 0 if ast.nil?

        case ast
        when Arel::Nodes::And
          # AND node has multiple conditions
          ast.children.count
        when Arel::Nodes::Or
          # OR node has multiple conditions
          ast.children.count
        else
          # Single condition (Equality, etc.)
          1
        end
      end
    end
  end
end
