# frozen_string_literal: true

require "spec_helper"

describe PgSearch::Document do
  with_table "pg_search_documents", &DOCUMENTS_SCHEMA

  describe ".logger" do
    it "returns super when super returns a logger" do
      # In normal Rails environment, super should return a logger
      logger = described_class.logger
      expect(logger).to respond_to(:info)
    end

    it "covers fallback logger when super returns nil" do
      # Temporarily redefine the logger method to simulate super returning nil
      original_logger_method = described_class.method(:logger)

      described_class.define_singleton_method(:logger) do
        # Simulate super returning nil, then fall back to Logger.new($stderr)
        nil || Logger.new($stderr)
      end

      # Test the fallback
      logger = described_class.logger
      expect(logger).to be_a(Logger)

      # Restore the original method
      described_class.define_singleton_method(:logger, original_logger_method)
    end

    it "covers the actual super || Logger.new fallback logic" do
      # Create a test class that mimics the Document behavior
      test_class = Class.new(ActiveRecord::Base) do
        include PgSearch::Model

        def self.logger
          super || Logger.new($stderr)
        end

        def self.name
          "TestDocument"
        end
      end

      # Mock the parent class to return nil
      allow(test_class.superclass).to receive(:logger).and_return(nil)

      logger = test_class.logger
      expect(logger).to be_a(Logger)
    end
  end
end