# frozen_string_literal: true

require "active_support/core_ext/module/delegation"

module PgSearch
  class ScopeOptions
    attr_reader :config, :feature_options, :model

    def initialize(config)
      @config = config
      @model = config.model
      @feature_options = config.feature_options
    end

    def apply(scope, &block)
      if block_given?
        # New inline approach for tenant scoping
        apply_with_tenant_scoping(scope, &block)
      else
        # Original subquery approach for backward compatibility
        apply_original(scope)
      end
    end

    private

    def apply_with_tenant_scoping(scope, &block)
      # Add association joins inline (not in subquery)
      scope = scope.joins(Arel.sql(subquery_join(&block))) if config.associations.any?

      # Only select all columns if no columns were explicitly selected
      if !scope.respond_to?(:select_values) || scope.select_values.empty?
        scope = scope.select("#{model.quoted_table_name}.*")
      end
      # If columns were explicitly selected, preserve them as-is

      # Wrap search conditions in Arel Grouping to ensure proper parentheses
      grouped_conditions = Arel::Nodes::Grouping.new(conditions)
      scope = scope.where(grouped_conditions)

      # Apply tenant scoping block for isolation LAST
      # Example: { |rel| rel.where(tenant_id: current_tenant.id) }
      begin
        original_where_count = count_where_conditions(scope)
        scope = block.call(scope)
        new_where_count = count_where_conditions(scope)

        # Verify tenant scoping actually added filtering to association, if a block was added we should infact see extra clauses chained
        if new_where_count <= original_where_count
          raise SecurityError, "Tenant scoping block must add WHERE conditions for security"
        end
      rescue SecurityError
        raise # bubble up
      rescue => e
        # Never allow association queries without tenant isolation when explicit block is passed down
        raise SecurityError, "Tenant scoping failed: #{e.message}. Query blocked for security."
      end

      # Order by primary key only - rank ordering will be added by with_pg_search_rank
      scope = scope.order(Arel.sql("#{order_within_rank}"))

      # Inject rank and order expressions into the scope for the inline module to use
      rank_expression = rank
      order_expression = order_within_rank

      scope.define_singleton_method(:pg_search_rank_expression) { rank_expression }
      scope.define_singleton_method(:pg_search_order_within_rank) { order_expression }

      # Extend with modules for compatibility
      scope.extend(WithPgSearchRankInline)
      scope.extend(WithPgSearchHighlight[feature_for(:tsearch)])
    end

    def apply_original(scope)
      scope = include_table_aliasing_for_rank(scope)
      rank_table_alias = scope.pg_search_rank_table_alias(include_counter: true)

      scope
        .joins(rank_join(rank_table_alias))
        .order(Arel.sql("#{rank_table_alias}.rank DESC, #{order_within_rank}"))
        .extend(WithPgSearchRank)
        .extend(WithPgSearchHighlight[feature_for(:tsearch)])
    end

    module WithPgSearchHighlight
      def self.[](tsearch)
        Module.new do
          include WithPgSearchHighlight

          define_method(:tsearch) { tsearch }
        end
      end

      def tsearch
        raise TypeError, "You need to instantiate this module with []"
      end

      def with_pg_search_highlight
        scope = self
        scope = scope.select("#{table_name}.*") unless scope.select_values.any?
        scope.select("(#{highlight}) AS pg_search_highlight")
      end

      def highlight
        tsearch.highlight.to_sql
      end
    end

    module WithPgSearchRank
      def with_pg_search_rank
        # Check if we're using inline approach (has pg_search_rank already selected)
        if select_values.any? && select_values.any? { |v| v.to_s.include?('pg_search_rank') }
          # Inline approach - rank is already added
          self
        else
          # Original subquery approach - need to add rank from subquery table
          scope = self
          scope = scope.select("#{table_name}.*") unless scope.select_values.any?
          scope.select("#{pg_search_rank_table_alias}.rank AS pg_search_rank")
        end
      end
    end

    module WithPgSearchRankInline
      def with_pg_search_rank
        scope = self

        # Add inline rank calculation using the injected expression
        scope = scope.select("(#{pg_search_rank_expression}) AS pg_search_rank")

        # Replace the order clause to include rank ordering
        scope = scope.reorder(Arel.sql("pg_search_rank DESC, #{pg_search_order_within_rank}"))

        scope
      end
    end

    module PgSearchRankTableAliasing
      def pg_search_rank_table_alias(include_counter: false)
        components = [arel_table.name]
        if include_counter
          count = increment_counter
          components << count if count > 0
        end

        Configuration.alias(components)
      end

      private

      def increment_counter
        @counter ||= 0
      ensure
        @counter += 1
      end
    end

    private

    delegate :connection, :quoted_table_name, to: :model

    def subquery(&block)
      # Start with base model
      relation = model.unscoped

      # If a block is given, wrap the base table in a subselect to force early filtering
      # This prevents PostgreSQL from doing full table scans on large tables
      if block_given?
        filtered_relation = block.call(model.unscoped)
        # Use from() to replace the FROM clause with a filtered subquery
        relation = relation.from("(#{filtered_relation.to_sql}) AS #{model.table_name}")
      end

      # Then add selects, joins, and search conditions
      relation
        .select("#{primary_key} AS pg_search_id")
        .select("#{rank} AS rank")
        .joins(subquery_join(&block))
        .where(conditions)
        .limit(nil)
        .offset(nil)
    end

    def conditions
      expressions =
        config.features
          .reject { |_feature_name, feature_options| feature_options && feature_options[:sort_only] }
          .map { |feature_name, _feature_options| feature_for(feature_name).conditions }

      or_node(expressions)
    end

    # https://github.com/rails/rails/pull/51492
    # :nocov:
    # standard:disable Lint/DuplicateMethods
    or_arity = Arel::Nodes::Or.instance_method(:initialize).arity
    case or_arity
    when 1
      def or_node(expressions)
        Arel::Nodes::Or.new(expressions)
      end
    when 2
      def or_node(expressions)
        expressions.inject { |accumulator, expression| Arel::Nodes::Or.new(accumulator, expression) }
      end
    else
      raise "Unsupported arity #{or_arity} for Arel::Nodes::Or#initialize"
    end
    # :nocov:
    # standard:enable Lint/DuplicateMethods

    def order_within_rank
      config.order_within_rank || "#{primary_key} ASC"
    end

    def primary_key
      "#{quoted_table_name}.#{connection.quote_column_name(model.primary_key)}"
    end

    def subquery_join(&block)
      if config.associations.any?
        config.associations.map do |association|
          association.join(primary_key, &block)
        end.join(" ")
      end
    end

    FEATURE_CLASSES = { # standard:disable Lint/UselessConstantScoping
      dmetaphone: Features::DMetaphone,
      tsearch: Features::TSearch,
      trigram: Features::Trigram
    }.freeze

    def feature_for(feature_name)
      feature_name = feature_name.to_sym
      feature_class = FEATURE_CLASSES[feature_name]

      raise ArgumentError, "Unknown feature: #{feature_name}" unless feature_class

      normalizer = Normalizer.new(config)

      feature_class.new(
        config.query,
        feature_options[feature_name],
        config.columns,
        config.model,
        normalizer
      )
    end

    def rank
      (config.ranking_sql || ":tsearch").gsub(/:(\w*)/) do
        feature_for(Regexp.last_match(1)).rank.to_sql
      end
    end

    def rank_join(rank_table_alias, &block)
      "INNER JOIN (#{subquery(&block).to_sql}) AS #{rank_table_alias} ON #{primary_key} = #{rank_table_alias}.pg_search_id"
    end

    def include_table_aliasing_for_rank(scope)
      return scope if scope.included_modules.include?(PgSearchRankTableAliasing)

      scope.all.spawn.tap do |new_scope|
        new_scope.instance_eval { extend PgSearchRankTableAliasing }
      end
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
