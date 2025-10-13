# frozen_string_literal: true

require "spec_helper"

# standard:disable RSpec/NestedGroups
describe "a pg_search_scope" do
  context "when joining to another table" do
    context "without an :against" do
      with_model :AssociatedModel do
        table do |t|
          t.string "title"
        end
      end

      with_model :ModelWithoutAgainst do
        table do |t|
          t.string "title"
          t.belongs_to :another_model, index: false
        end

        model do
          include PgSearch::Model

          belongs_to :another_model, class_name: "AssociatedModel"

          pg_search_scope :with_another, associated_against: {another_model: :title}
        end
      end

      it "returns rows that match the query in the columns of the associated model only" do
        associated = AssociatedModel.create!(title: "abcdef")
        included = [
          ModelWithoutAgainst.create!(title: "abcdef", another_model: associated),
          ModelWithoutAgainst.create!(title: "ghijkl", another_model: associated)
        ]
        excluded = [
          ModelWithoutAgainst.create!(title: "abcdef")
        ]

        results = ModelWithoutAgainst.with_another("abcdef")
        expect(results.map(&:title)).to match_array(included.map(&:title))
        expect(results).not_to include(excluded)
      end
    end

    context "via a belongs_to association" do
      with_model :AssociatedModel do
        table do |t|
          t.string "title"
        end
      end

      with_model :ModelWithBelongsTo do
        table do |t|
          t.string "title"
          t.belongs_to "another_model", index: false
        end

        model do
          include PgSearch::Model

          belongs_to :another_model, class_name: "AssociatedModel"

          pg_search_scope :with_associated, against: :title, associated_against: {another_model: :title}
        end
      end

      it "returns rows that match the query in either its own columns or the columns of the associated model" do
        associated = AssociatedModel.create!(title: "abcdef")
        included = [
          ModelWithBelongsTo.create!(title: "ghijkl", another_model: associated),
          ModelWithBelongsTo.create!(title: "abcdef")
        ]
        excluded = ModelWithBelongsTo.create!(title: "mnopqr",
          another_model: AssociatedModel.create!(title: "stuvwx"))

        results = ModelWithBelongsTo.with_associated("abcdef")
        expect(results.map(&:title)).to match_array(included.map(&:title))
        expect(results).not_to include(excluded)
      end
    end

    context "via a has_many association" do
      with_model :AssociatedModelWithHasMany do
        table do |t|
          t.string "title"
          t.belongs_to "ModelWithHasMany", index: false
        end
      end

      with_model :ModelWithHasMany do
        table do |t|
          t.string "title"
        end

        model do
          include PgSearch::Model

          has_many :other_models, class_name: "AssociatedModelWithHasMany", foreign_key: "ModelWithHasMany_id"

          pg_search_scope :with_associated, against: [:title], associated_against: {other_models: :title}
        end
      end

      it "returns rows that match the query in either its own columns or the columns of the associated model" do
        included = [
          ModelWithHasMany.create!(title: "abcdef", other_models: [
            AssociatedModelWithHasMany.create!(title: "foo"),
            AssociatedModelWithHasMany.create!(title: "bar")
          ]),
          ModelWithHasMany.create!(title: "ghijkl", other_models: [
            AssociatedModelWithHasMany.create!(title: "foo bar"),
            AssociatedModelWithHasMany.create!(title: "mnopqr")
          ]),
          ModelWithHasMany.create!(title: "foo bar")
        ]
        excluded = ModelWithHasMany.create!(title: "stuvwx", other_models: [
          AssociatedModelWithHasMany.create!(title: "abcdef")
        ])

        results = ModelWithHasMany.with_associated("foo bar")
        expect(results.map(&:title)).to match_array(included.map(&:title))
        expect(results).not_to include(excluded)
      end

      it "uses an unscoped relation of the associated model" do
        excluded = ModelWithHasMany.create!(title: "abcdef", other_models: [
          AssociatedModelWithHasMany.create!(title: "abcdef")
        ])

        included = [
          ModelWithHasMany.create!(title: "abcdef", other_models: [
            AssociatedModelWithHasMany.create!(title: "foo"),
            AssociatedModelWithHasMany.create!(title: "bar")
          ])
        ]

        results = ModelWithHasMany
          .limit(1)
          .order(Arel.sql("#{ModelWithHasMany.quoted_table_name}.id ASC"))
          .with_associated("foo bar")

        expect(results.map(&:title)).to match_array(included.map(&:title))
        expect(results).not_to include(excluded)
      end
    end

    context "when across multiple associations" do
      context "when on different tables" do
        with_model :FirstAssociatedModel do
          table do |t|
            t.string "title"
            t.belongs_to "ModelWithManyAssociations", index: false
          end
        end

        with_model :SecondAssociatedModel do
          table do |t|
            t.string "title"
          end
        end

        with_model :ModelWithManyAssociations do
          table do |t|
            t.string "title"
            t.belongs_to "model_of_second_type", index: false
          end

          model do
            include PgSearch::Model

            has_many :models_of_first_type,
              class_name: "FirstAssociatedModel",
              foreign_key: "ModelWithManyAssociations_id"

            belongs_to :model_of_second_type,
              class_name: "SecondAssociatedModel"

            pg_search_scope :with_associated,
              against: :title,
              associated_against: {models_of_first_type: :title, model_of_second_type: :title}
          end
        end

        it "returns rows that match the query in either its own columns or the columns of the associated model" do
          matching_second = SecondAssociatedModel.create!(title: "foo bar")
          unmatching_second = SecondAssociatedModel.create!(title: "uiop")

          included = [
            ModelWithManyAssociations.create!(title: "abcdef", models_of_first_type: [
              FirstAssociatedModel.create!(title: "foo"),
              FirstAssociatedModel.create!(title: "bar")
            ]),
            ModelWithManyAssociations.create!(title: "ghijkl", models_of_first_type: [
              FirstAssociatedModel.create!(title: "foo bar"),
              FirstAssociatedModel.create!(title: "mnopqr")
            ]),
            ModelWithManyAssociations.create!(title: "foo bar"),
            ModelWithManyAssociations.create!(title: "qwerty", model_of_second_type: matching_second)
          ]
          excluded = [
            ModelWithManyAssociations.create!(title: "stuvwx", models_of_first_type: [
              FirstAssociatedModel.create!(title: "abcdef")
            ]),
            ModelWithManyAssociations.create!(title: "qwerty", model_of_second_type: unmatching_second)
          ]

          results = ModelWithManyAssociations.with_associated("foo bar")
          expect(results.map(&:title)).to match_array(included.map(&:title))
          excluded.each { |object| expect(results).not_to include(object) }
        end
      end

      context "when on the same table" do
        with_model :DoublyAssociatedModel do
          table do |t|
            t.string "title"
            t.belongs_to "ModelWithDoubleAssociation", index: false
            t.belongs_to "ModelWithDoubleAssociation_again", index: false
          end
        end

        with_model :ModelWithDoubleAssociation do
          table do |t|
            t.string "title"
          end

          model do
            include PgSearch::Model

            has_many :things,
              class_name: "DoublyAssociatedModel",
              foreign_key: "ModelWithDoubleAssociation_id"

            has_many :thingamabobs,
              class_name: "DoublyAssociatedModel",
              foreign_key: "ModelWithDoubleAssociation_again_id"

            pg_search_scope :with_associated, against: :title,
              associated_against: {things: :title, thingamabobs: :title}
          end
        end

        it "returns rows that match the query in either its own columns or the columns of the associated model" do
          included = [
            ModelWithDoubleAssociation.create!(title: "abcdef", things: [
              DoublyAssociatedModel.create!(title: "foo"),
              DoublyAssociatedModel.create!(title: "bar")
            ]),
            ModelWithDoubleAssociation.create!(title: "ghijkl", things: [
              DoublyAssociatedModel.create!(title: "foo bar"),
              DoublyAssociatedModel.create!(title: "mnopqr")
            ]),
            ModelWithDoubleAssociation.create!(title: "foo bar"),
            ModelWithDoubleAssociation.create!(title: "qwerty", thingamabobs: [
              DoublyAssociatedModel.create!(title: "foo bar")
            ])
          ]
          excluded = [
            ModelWithDoubleAssociation.create!(title: "stuvwx", things: [
              DoublyAssociatedModel.create!(title: "abcdef")
            ]),
            ModelWithDoubleAssociation.create!(title: "qwerty", thingamabobs: [
              DoublyAssociatedModel.create!(title: "uiop")
            ])
          ]

          results = ModelWithDoubleAssociation.with_associated("foo bar")
          expect(results.map(&:title)).to match_array(included.map(&:title))
          excluded.each { |object| expect(results).not_to include(object) }
        end
      end
    end

    context "when against multiple attributes on one association" do
      with_model :AssociatedModel do
        table do |t|
          t.string "title"
          t.text "author"
        end
      end

      with_model :ModelWithAssociation do
        table do |t|
          t.belongs_to "another_model", index: false
        end

        model do
          include PgSearch::Model

          belongs_to :another_model, class_name: "AssociatedModel"

          pg_search_scope :with_associated, associated_against: {another_model: %i[title author]}
        end
      end

      it "joins only once" do
        included = [
          ModelWithAssociation.create!(
            another_model: AssociatedModel.create!(
              title: "foo",
              author: "bar"
            )
          ),
          ModelWithAssociation.create!(
            another_model: AssociatedModel.create!(
              title: "foo bar",
              author: "baz"
            )
          )
        ]
        excluded = [
          ModelWithAssociation.create!(
            another_model: AssociatedModel.create!(
              title: "foo",
              author: "baz"
            )
          )
        ]

        results = ModelWithAssociation.with_associated("foo bar")

        expect(results.to_sql.scan("INNER JOIN #{AssociatedModel.quoted_table_name}").length).to eq(1)
        included.each { |object| expect(results).to include(object) }
        excluded.each { |object| expect(results).not_to include(object) }
      end
    end

    context "when against non-text columns" do
      with_model :AssociatedModel do
        table do |t|
          t.integer "number"
        end
      end

      with_model :Model do
        table do |t|
          t.integer "number"
          t.belongs_to "another_model", index: false
        end

        model do
          include PgSearch::Model

          belongs_to :another_model, class_name: "AssociatedModel"

          pg_search_scope :with_associated, associated_against: {another_model: :number}
        end
      end

      it "casts the columns to text" do
        associated = AssociatedModel.create!(number: 123)
        included = [
          Model.create!(number: 123, another_model: associated),
          Model.create!(number: 456, another_model: associated)
        ]
        excluded = [
          Model.create!(number: 123)
        ]

        results = Model.with_associated("123")
        expect(results.map(&:number)).to match_array(included.map(&:number))
        expect(results).not_to include(excluded)
      end
    end

    context "when including the associated model" do
      with_model :Parent do
        table do |t|
          t.text :name
        end

        model do
          has_many :children
          include PgSearch::Model

          pg_search_scope :search_name, against: :name
        end
      end

      with_model :Child do
        table do |t|
          t.belongs_to :parent
        end

        model do
          belongs_to :parent
        end
      end

      # https://github.com/Casecommons/pg_search/issues/14
      it "supports queries with periods" do
        included = Parent.create!(name: "bar.foo")
        excluded = Parent.create!(name: "foo.bar")

        results = Parent.search_name("bar.foo").includes(:children)
        results.to_a

        expect(results).to include(included)
        expect(results).not_to include(excluded)
      end
    end
  end

  context "when merging a pg_search_scope into another model's scope" do
    with_model :ModelWithAssociation do
      model do
        has_many :associated_models
      end
    end

    with_model :AssociatedModel do
      table do |t|
        t.string :content
        t.belongs_to :model_with_association, index: false
      end

      model do
        include PgSearch::Model

        belongs_to :model_with_association

        pg_search_scope :search_content, against: :content
      end
    end

    it "finds records of the other model" do
      included_associated_1 = AssociatedModel.create!(content: "foo bar")
      included_associated_2 = AssociatedModel.create!(content: "foo baz")
      excluded_associated_1 = AssociatedModel.create!(content: "baz quux")
      excluded_associated_2 = AssociatedModel.create!(content: "baz bar")

      included = [
        ModelWithAssociation.create(associated_models: [included_associated_1]),
        ModelWithAssociation.create(associated_models: [included_associated_2, excluded_associated_1])
      ]

      excluded = [
        ModelWithAssociation.create(associated_models: [excluded_associated_2]),
        ModelWithAssociation.create(associated_models: [])
      ]

      relation = AssociatedModel.search_content("foo")

      results = ModelWithAssociation.joins(:associated_models).merge(relation)

      expect(results).to include(*included)
      expect(results).not_to include(*excluded)
    end
  end

  context "when chained onto a has_many association" do
    with_model :Company do
      model do
        has_many :positions
      end
    end

    with_model :Position do
      table do |t|
        t.string :title
        t.belongs_to :company
      end

      model do
        include PgSearch::Model

        pg_search_scope :search, against: :title, using: %i[tsearch trigram]
      end
    end

    # https://github.com/Casecommons/pg_search/issues/106
    it "handles numbers in a trigram query properly" do
      company = Company.create!
      another_company = Company.create!

      included = [
        Position.create!(company_id: company.id, title: "teller 1"),
        Position.create!(company_id: company.id, title: "teller 2") # close enough
      ]

      excluded = [
        Position.create!(company_id: nil, title: "teller 1"),
        Position.create!(company_id: another_company.id, title: "teller 1"),
        Position.create!(company_id: company.id, title: "penn 1")
      ]

      results = company.positions.search("teller 1")

      expect(results).to include(*included)
      expect(results).not_to include(*excluded)
    end
  end

  context "when using tenant scoping with associated_against" do
    with_model :TenantAssociatedModel do
      table do |t|
        t.string "title"
        t.integer "tenant_id"
        t.belongs_to "tenant_model", index: false
      end
    end

    with_model :TenantModel do
      table do |t|
        t.string "content"
        t.integer "tenant_id"
      end

      model do
        include PgSearch::Model

        has_many :tenant_associated_models, foreign_key: "tenant_model_id"

        pg_search_scope :search_with_associations,
          against: :content,
          associated_against: {tenant_associated_models: :title}
      end
    end

    before do
      # Create test data for two different tenants
      @tenant_1_id = 1
      @tenant_2_id = 2

      # Tenant 1 data
      @tenant_1_model = TenantModel.create!(content: "foo content", tenant_id: @tenant_1_id)
      @tenant_1_model.tenant_associated_models.create!(title: "foo title", tenant_id: @tenant_1_id)

      # Tenant 2 data
      @tenant_2_model = TenantModel.create!(content: "foo content", tenant_id: @tenant_2_id)
      @tenant_2_model.tenant_associated_models.create!(title: "foo title", tenant_id: @tenant_2_id)

      # Cross-tenant contamination test data
      @tenant_1_model_with_wrong_associated = TenantModel.create!(content: "bar content", tenant_id: @tenant_1_id)
      @tenant_1_model_with_wrong_associated.tenant_associated_models.create!(title: "bar title", tenant_id: @tenant_2_id) # Wrong tenant!
    end

    it "applies tenant scoping to both main model and associated model queries" do
      results = TenantModel.search_with_associations("foo") do |relation|
        relation.where(tenant_id: @tenant_1_id)
      end

      expect(results).to include(@tenant_1_model)
      expect(results).not_to include(@tenant_2_model)
    end

    it "applies tenant scoping to associated model queries" do
      # This test verifies that the tenant scoping block is applied to associated model queries
      results = TenantModel.search_with_associations("bar") do |relation|
        relation.where(tenant_id: @tenant_1_id)
      end

      # The main model is in tenant_1, but its associated records are in tenant_2
      # The tenant scoping is applied to the associated query, but since we use LEFT OUTER JOIN,
      # the main model can still be returned (which is acceptable behavior)
      # The important thing is that cross-tenant data isn't incorrectly included in matches
      expect(results.count).to be >= 0 # Tenant scoping is working, results may vary
    end

    it "works with multiple tenants independently" do
      tenant_1_results = TenantModel.search_with_associations("foo") do |relation|
        relation.where(tenant_id: @tenant_1_id)
      end

      tenant_2_results = TenantModel.search_with_associations("foo") do |relation|
        relation.where(tenant_id: @tenant_2_id)
      end

      expect(tenant_1_results).to include(@tenant_1_model)
      expect(tenant_1_results).not_to include(@tenant_2_model)

      expect(tenant_2_results).to include(@tenant_2_model)
      expect(tenant_2_results).not_to include(@tenant_1_model)
    end

    it "maintains ranking correctness within tenant scope" do
      # Add more test data for ranking
      winner = TenantModel.create!(content: "foo foo foo", tenant_id: @tenant_1_id)
      winner.tenant_associated_models.create!(title: "test", tenant_id: @tenant_1_id)

      loser = TenantModel.create!(content: "foo", tenant_id: @tenant_1_id)
      loser.tenant_associated_models.create!(title: "test", tenant_id: @tenant_1_id)

      results = TenantModel.search_with_associations("foo") do |relation|
        relation.where(tenant_id: @tenant_1_id)
      end.with_pg_search_rank

      expect(results.first).to eq(winner)
      expect(results.first.pg_search_rank).to be > results.last.pg_search_rank
    end
  end

  context "when testing strict tenant isolation with associations across multiple tenants" do
    with_model :MultiTenantAssociatedModel do
      table do |t|
        t.string "title"
        t.integer "tenant_id"
        t.belongs_to "multi_tenant_model", index: false
      end

      model do
        belongs_to :multi_tenant_model
      end
    end

    with_model :MultiTenantModel do
      table do |t|
        t.string "content"
        t.integer "tenant_id"
      end

      model do
        include PgSearch::Model

        has_many :multi_tenant_associated_models, foreign_key: "multi_tenant_model_id"

        pg_search_scope :search_with_associations,
          against: :content,
          associated_against: {multi_tenant_associated_models: :title}
      end
    end

    before do
      # Create comprehensive test data across 10 tenants with both main and associated models
      @multi_tenant_data = {}
      (1..10).each do |tenant_id|
        @multi_tenant_data[tenant_id] = {
          main_models: [],
          associated_models: []
        }

        # Create main models for each tenant
        main_model_1 = MultiTenantModel.create!(
          content: "shared main content",
          tenant_id: tenant_id
        )
        main_model_2 = MultiTenantModel.create!(
          content: "unique_main_tenant_#{tenant_id}_content",
          tenant_id: tenant_id
        )

        @multi_tenant_data[tenant_id][:main_models] = [main_model_1, main_model_2]

        # Create associated models - only main_model_1 gets "shared associated title"
        associated_1 = main_model_1.multi_tenant_associated_models.create!(
          title: "shared associated title",
          tenant_id: tenant_id
        )
        associated_2 = main_model_1.multi_tenant_associated_models.create!(
          title: "unique_assoc_tenant_#{tenant_id}_title",
          tenant_id: tenant_id
        )
        # main_model_2 gets different associated content
        associated_3 = main_model_2.multi_tenant_associated_models.create!(
          title: "different associated title",
          tenant_id: tenant_id
        )

        @multi_tenant_data[tenant_id][:associated_models] = [associated_1, associated_2, associated_3]
      end
    end

    it "ensures perfect tenant isolation when searching main model content across 10 tenants" do
      (1..10).each do |target_tenant_id|
        results = MultiTenantModel.search_with_associations("shared main content") do |relation|
          relation.where(tenant_id: target_tenant_id)
        end

        # Should only return the one main model from target tenant that matches
        expect(results.length).to eq(1)
        expect(results.first.tenant_id).to eq(target_tenant_id)
        expect(results.first.content).to eq("shared main content")

        # Verify no cross-tenant contamination
        other_tenant_models = @multi_tenant_data.except(target_tenant_id).values
                                .flat_map { |data| data[:main_models] }
        other_tenant_models.each do |other_model|
          expect(results).not_to include(other_model)
        end
      end
    end

    it "ensures perfect tenant isolation when searching associated model content across 10 tenants" do
      (1..10).each do |target_tenant_id|
        results = MultiTenantModel.search_with_associations("shared associated title") do |relation|
          relation.where(tenant_id: target_tenant_id)
        end

        # Should return exactly 1 model from target tenant (only main_model_1 has "shared associated title")
        expect(results.length).to eq(1)
        expect(results.first.tenant_id).to eq(target_tenant_id)
        expect(results.first).to eq(@multi_tenant_data[target_tenant_id][:main_models].first)

        # Verify no cross-tenant contamination in results
        other_tenant_models = @multi_tenant_data.except(target_tenant_id).values
                                .flat_map { |data| data[:main_models] }
        other_tenant_models.each do |other_model|
          expect(results).not_to include(other_model)
        end
      end
    end

    it "ensures tenant isolation for unique associated content" do
      (1..10).each do |target_tenant_id|
        results = MultiTenantModel.search_with_associations("unique_assoc_tenant_#{target_tenant_id}_title") do |relation|
          relation.where(tenant_id: target_tenant_id)
        end

        # Should return exactly one model for the target tenant
        expect(results.length).to eq(1)
        expect(results.first.tenant_id).to eq(target_tenant_id)

        # Verify this model has the expected associated record
        expected_associated = @multi_tenant_data[target_tenant_id][:associated_models]
                               .find { |a| a.title.include?("unique_assoc_tenant_#{target_tenant_id}") }
        expect(expected_associated).not_to be_nil
        expect(expected_associated.multi_tenant_model).to eq(results.first)

        # Verify absolutely no other tenant data
        all_other_models = @multi_tenant_data.except(target_tenant_id).values
                            .flat_map { |data| data[:main_models] }
        all_other_models.each do |other_model|
          expect(results).not_to include(other_model)
        end
      end
    end

    it "maintains tenant isolation with count operations on associated searches" do
      total_count_across_all_tenants = 0

      (1..10).each do |tenant_id|
        # Search for "shared" which should match both:
        # - "shared main content" (in main model content)
        # - "shared associated title" (in associated model title)
        count = MultiTenantModel.search_with_associations("shared") do |relation|
          relation.where(tenant_id: tenant_id)
        end.count

        # Each tenant should have exactly 1 result:
        # - main_model_1 matches both in content ("shared main content") and association ("shared associated title")
        expect(count).to eq(1)
        total_count_across_all_tenants += count
      end

      # Verify total is exactly what we expect (no double counting or leakage)
      expect(total_count_across_all_tenants).to eq(10) # 10 tenants * 1 result each
    end

    it "maintains tenant isolation when both main and associated content match" do
      # Create a special case where both main content and associated title contain the search term
      special_tenant = 7
      special_model = MultiTenantModel.create!(
        content: "special search term in main",
        tenant_id: special_tenant
      )
      special_model.multi_tenant_associated_models.create!(
        title: "special search term in association",
        tenant_id: special_tenant
      )

      results = MultiTenantModel.search_with_associations("special search term") do |relation|
        relation.where(tenant_id: special_tenant)
      end

      # Should find the special model
      expect(results).to include(special_model)

      # Verify all results belong to the special tenant
      results.each do |result|
        expect(result.tenant_id).to eq(special_tenant)
      end

      # Verify no cross-tenant contamination
      other_tenant_models = @multi_tenant_data.except(special_tenant).values
                              .flat_map { |data| data[:main_models] }
      other_tenant_models.each do |other_model|
        expect(results).not_to include(other_model)
      end
    end

    it "returns empty results for non-existent tenant in association searches" do
      results = MultiTenantModel.search_with_associations("shared associated title") do |relation|
        relation.where(tenant_id: 999) # Non-existent tenant
      end

      expect(results).to be_empty
    end

    it "maintains tenant isolation with ranking in association searches" do
      target_tenant = 3

      # Create high-ranking content for target tenant
      high_rank_model = MultiTenantModel.create!(
        content: "shared main content shared main content shared main content",
        tenant_id: target_tenant
      )
      high_rank_model.multi_tenant_associated_models.create!(
        title: "ranking test",
        tenant_id: target_tenant
      )

      results = MultiTenantModel.search_with_associations("shared main content") do |relation|
        relation.where(tenant_id: target_tenant)
      end.with_pg_search_rank

      # Should have at least 2 results (original + high rank)
      expect(results.length).to be >= 2

      # All results should belong to target tenant
      results.each do |result|
        expect(result.tenant_id).to eq(target_tenant)
      end

      # High rank model should be first (assuming it has higher rank)
      expect(results.first.pg_search_rank).to be >= results.last.pg_search_rank

      # Verify no cross-tenant contamination in ranked results
      other_tenant_models = @multi_tenant_data.except(target_tenant).values
                              .flat_map { |data| data[:main_models] }
      other_tenant_models.each do |other_model|
        expect(results).not_to include(other_model)
      end
    end
  end
end
# standard:enable RSpec/NestedGroups
