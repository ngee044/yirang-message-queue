#include "TestHelpers.h"
#include "MessageValidator.h"
#include <gtest/gtest.h>

class MessageValidatorTest : public ::testing::Test
{
protected:
	MessageValidator validator_;

	void SetUp() override
	{
		init_test_logger();
	}

	MessageEnvelope make_envelope(const std::string& queue, const std::string& payload_json)
	{
		MessageEnvelope env;
		env.key = "test-key";
		env.message_id = "test-msg-id";
		env.queue = queue;
		env.payload_json = payload_json;
		env.attributes_json = "{}";
		env.priority = 0;
		env.attempt = 1;
		env.created_at_ms = 0;
		env.available_at_ms = 0;
		env.target_consumer_id = "";
		return env;
	}
};

// =============================================================================
// SchemaManagement
// =============================================================================

TEST_F(MessageValidatorTest, RegisterSchema)
{
	MessageSchema schema;
	schema.name = "telemetry";
	schema.description = "Telemetry message schema";
	schema.rules = {};

	validator_.register_schema("telemetry", schema);

	EXPECT_TRUE(validator_.has_schema("telemetry"));
}

TEST_F(MessageValidatorTest, UnregisterSchema)
{
	MessageSchema schema;
	schema.name = "telemetry";
	schema.description = "Telemetry message schema";
	schema.rules = {};

	validator_.register_schema("telemetry", schema);
	EXPECT_TRUE(validator_.has_schema("telemetry"));

	validator_.unregister_schema("telemetry");
	EXPECT_FALSE(validator_.has_schema("telemetry"));
}

TEST_F(MessageValidatorTest, HasSchemaReturnsFalseForUnregistered)
{
	EXPECT_FALSE(validator_.has_schema("nonexistent"));
}

TEST_F(MessageValidatorTest, GetSchemaReturnsRegisteredSchema)
{
	MessageSchema schema;
	schema.name = "telemetry";
	schema.description = "Telemetry message schema";
	schema.rules = {};

	validator_.register_schema("telemetry", schema);

	auto retrieved = validator_.get_schema("telemetry");
	ASSERT_TRUE(retrieved.has_value());
	EXPECT_EQ(retrieved->name, "telemetry");
	EXPECT_EQ(retrieved->description, "Telemetry message schema");
}

TEST_F(MessageValidatorTest, GetSchemaReturnsNulloptForUnregistered)
{
	auto retrieved = validator_.get_schema("nonexistent");
	EXPECT_FALSE(retrieved.has_value());
}

TEST_F(MessageValidatorTest, NoSchemaReturnsValid)
{
	auto result = validator_.validate_json(R"({"temp": 36.5})", "unregistered_queue");
	EXPECT_TRUE(result.valid);
	EXPECT_TRUE(result.errors.empty());
}

// =============================================================================
// Required rules
// =============================================================================

TEST_F(MessageValidatorTest, RequiredFieldPresentPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
	EXPECT_TRUE(result.errors.empty());
}

TEST_F(MessageValidatorTest, RequiredFieldAbsentFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"humidity": 50})", "test_queue");
	EXPECT_FALSE(result.valid);
	EXPECT_FALSE(result.errors.empty());
}

TEST_F(MessageValidatorTest, RequiredFieldNullPasses)
{
	// The validator's Required rule checks field existence via contains(),
	// so null is treated as "present"
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": null})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, RequiredFieldCustomErrorMessage)
{
	auto rule = MessageValidator::required("sensor_id");
	rule.error_message = "sensor_id is mandatory";

	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { rule };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_FALSE(result.valid);
	ASSERT_FALSE(result.errors.empty());

	bool found_custom_message = false;
	for (const auto& err : result.errors)
	{
		if (err.find("sensor_id") != std::string::npos)
		{
			found_custom_message = true;
			break;
		}
	}
	EXPECT_TRUE(found_custom_message);
}

// =============================================================================
// Type rules - string
// =============================================================================

TEST_F(MessageValidatorTest, TypeStringMatchPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_string("name") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "sensor-01"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, TypeStringMismatchFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_string("name") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": 123})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, TypeStringFieldAbsentSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_string("name") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Type rules - number
// =============================================================================

TEST_F(MessageValidatorTest, TypeNumberMatchPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_number("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, TypeNumberMismatchFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_number("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": "hot"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, TypeNumberFieldAbsentSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_number("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "sensor"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Type rules - boolean
// =============================================================================

TEST_F(MessageValidatorTest, TypeBooleanMatchPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_boolean("active") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"active": true})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, TypeBooleanMismatchFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_boolean("active") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"active": "yes"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, TypeBooleanFieldAbsentSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_boolean("active") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Type rules - array
// =============================================================================

TEST_F(MessageValidatorTest, TypeArrayMatchPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_array("tags") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"tags": ["a", "b"]})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, TypeArrayMismatchFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_array("tags") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"tags": "not-an-array"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, TypeArrayFieldAbsentSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_array("tags") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Type rules - object
// =============================================================================

TEST_F(MessageValidatorTest, TypeObjectMatchPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_object("metadata") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"metadata": {"key": "val"}})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, TypeObjectMismatchFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_object("metadata") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"metadata": "not-an-object"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, TypeObjectFieldAbsentSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::type_object("metadata") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Length rules - MinLength
// =============================================================================

TEST_F(MessageValidatorTest, MinLengthAtBoundaryPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_length("name", 3) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "abc"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, MinLengthBelowBoundaryFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_length("name", 3) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "ab"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, MinLengthNonStringSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_length("count", 3) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"count": 42})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Length rules - MaxLength
// =============================================================================

TEST_F(MessageValidatorTest, MaxLengthAtBoundaryPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_length("name", 5) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "abcde"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, MaxLengthAboveBoundaryFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_length("name", 5) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "abcdef"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, MaxLengthNonStringSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_length("count", 5) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"count": 999999})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Value rules - MinValue
// =============================================================================

TEST_F(MessageValidatorTest, MinValueAtBoundaryPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_value("temp", 0.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 0.0})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, MinValueBelowBoundaryFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_value("temp", 0.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": -1.0})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, MinValueAboveBoundaryPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_value("temp", 0.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 100.0})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, MinValueNonNumberSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::min_value("name", 0.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "sensor"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Value rules - MaxValue
// =============================================================================

TEST_F(MessageValidatorTest, MaxValueAtBoundaryPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_value("temp", 100.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 100.0})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, MaxValueAboveBoundaryFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_value("temp", 100.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 101.0})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, MaxValueBelowBoundaryPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_value("temp", 100.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 50.0})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, MaxValueNonNumberSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::max_value("name", 100.0) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "sensor"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Enum rules
// =============================================================================

TEST_F(MessageValidatorTest, EnumValueInListPasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::enum_values("status", {"active", "inactive", "pending"}) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"status": "active"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, EnumValueNotInListFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::enum_values("status", {"active", "inactive", "pending"}) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"status": "unknown"})", "test_queue");
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, EnumFieldAbsentSkips)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::enum_values("status", {"active", "inactive"}) };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// ValidateJson - invalid JSON
// =============================================================================

TEST_F(MessageValidatorTest, ValidateJsonInvalidJsonFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json("{invalid json", "test_queue");
	EXPECT_FALSE(result.valid);
	EXPECT_FALSE(result.errors.empty());
}

TEST_F(MessageValidatorTest, ValidateJsonEmptyStringFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json("", "test_queue");
	EXPECT_FALSE(result.valid);
}

// =============================================================================
// ValidateJson - multiple rule violations
// =============================================================================

TEST_F(MessageValidatorTest, ValidateJsonMultipleRuleViolations)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = {
		MessageValidator::required("temp"),
		MessageValidator::required("sensor_id"),
		MessageValidator::required("timestamp")
	};

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"other": "value"})", "test_queue");
	EXPECT_FALSE(result.valid);
	EXPECT_GE(result.errors.size(), 3u);
}

// =============================================================================
// Validate with MessageEnvelope
// =============================================================================

TEST_F(MessageValidatorTest, ValidateEnvelopePasses)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto envelope = make_envelope("test_queue", R"({"temp": 36.5})");
	auto result = validator_.validate(envelope);
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, ValidateEnvelopeFails)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = { MessageValidator::required("temp") };

	validator_.register_schema("test_queue", schema);

	auto envelope = make_envelope("test_queue", R"({"humidity": 50})");
	auto result = validator_.validate(envelope);
	EXPECT_FALSE(result.valid);
}

TEST_F(MessageValidatorTest, ValidateEnvelopeNoSchemaReturnsValid)
{
	auto envelope = make_envelope("unregistered_queue", R"({"temp": 36.5})");
	auto result = validator_.validate(envelope);
	EXPECT_TRUE(result.valid);
}

// =============================================================================
// Static builders
// =============================================================================

TEST_F(MessageValidatorTest, BuilderRequiredReturnsCorrectRule)
{
	auto rule = MessageValidator::required("sensor_id");
	EXPECT_EQ(rule.field, "sensor_id");
	EXPECT_EQ(rule.type, ValidationRuleType::Required);
}

TEST_F(MessageValidatorTest, BuilderTypeStringReturnsCorrectRule)
{
	auto rule = MessageValidator::type_string("name");
	EXPECT_EQ(rule.field, "name");
	EXPECT_EQ(rule.type, ValidationRuleType::Type);
	EXPECT_EQ(rule.expected_type, "string");
}

TEST_F(MessageValidatorTest, BuilderTypeNumberReturnsCorrectRule)
{
	auto rule = MessageValidator::type_number("temp");
	EXPECT_EQ(rule.field, "temp");
	EXPECT_EQ(rule.type, ValidationRuleType::Type);
	EXPECT_EQ(rule.expected_type, "number");
}

TEST_F(MessageValidatorTest, BuilderTypeBooleanReturnsCorrectRule)
{
	auto rule = MessageValidator::type_boolean("active");
	EXPECT_EQ(rule.field, "active");
	EXPECT_EQ(rule.type, ValidationRuleType::Type);
	EXPECT_EQ(rule.expected_type, "boolean");
}

TEST_F(MessageValidatorTest, BuilderTypeArrayReturnsCorrectRule)
{
	auto rule = MessageValidator::type_array("tags");
	EXPECT_EQ(rule.field, "tags");
	EXPECT_EQ(rule.type, ValidationRuleType::Type);
	EXPECT_EQ(rule.expected_type, "array");
}

TEST_F(MessageValidatorTest, BuilderTypeObjectReturnsCorrectRule)
{
	auto rule = MessageValidator::type_object("metadata");
	EXPECT_EQ(rule.field, "metadata");
	EXPECT_EQ(rule.type, ValidationRuleType::Type);
	EXPECT_EQ(rule.expected_type, "object");
}

TEST_F(MessageValidatorTest, BuilderMinLengthReturnsCorrectRule)
{
	auto rule = MessageValidator::min_length("name", 5);
	EXPECT_EQ(rule.field, "name");
	EXPECT_EQ(rule.type, ValidationRuleType::MinLength);
	EXPECT_EQ(rule.min_length, 5);
}

TEST_F(MessageValidatorTest, BuilderMaxLengthReturnsCorrectRule)
{
	auto rule = MessageValidator::max_length("name", 100);
	EXPECT_EQ(rule.field, "name");
	EXPECT_EQ(rule.type, ValidationRuleType::MaxLength);
	EXPECT_EQ(rule.max_length, 100);
}

TEST_F(MessageValidatorTest, BuilderMinValueReturnsCorrectRule)
{
	auto rule = MessageValidator::min_value("temp", -40.0);
	EXPECT_EQ(rule.field, "temp");
	EXPECT_EQ(rule.type, ValidationRuleType::MinValue);
	EXPECT_DOUBLE_EQ(rule.min_value, -40.0);
}

TEST_F(MessageValidatorTest, BuilderMaxValueReturnsCorrectRule)
{
	auto rule = MessageValidator::max_value("temp", 125.0);
	EXPECT_EQ(rule.field, "temp");
	EXPECT_EQ(rule.type, ValidationRuleType::MaxValue);
	EXPECT_DOUBLE_EQ(rule.max_value, 125.0);
}

TEST_F(MessageValidatorTest, BuilderEnumValuesReturnsCorrectRule)
{
	auto rule = MessageValidator::enum_values("status", {"active", "inactive", "pending"});
	EXPECT_EQ(rule.field, "status");
	EXPECT_EQ(rule.type, ValidationRuleType::Enum);
	ASSERT_EQ(rule.enum_values.size(), 3u);
	EXPECT_EQ(rule.enum_values[0], "active");
	EXPECT_EQ(rule.enum_values[1], "inactive");
	EXPECT_EQ(rule.enum_values[2], "pending");
}

// =============================================================================
// Combined rules
// =============================================================================

TEST_F(MessageValidatorTest, CombinedRequiredAndTypeRules)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = {
		MessageValidator::required("temp"),
		MessageValidator::type_number("temp"),
		MessageValidator::required("name"),
		MessageValidator::type_string("name")
	};

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5, "name": "sensor-01"})", "test_queue");
	EXPECT_TRUE(result.valid);
}

TEST_F(MessageValidatorTest, CombinedRequiredAndTypeRulesFail)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = {
		MessageValidator::required("temp"),
		MessageValidator::type_number("temp"),
		MessageValidator::required("name"),
		MessageValidator::type_string("name")
	};

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": "not_a_number", "name": 123})", "test_queue");
	EXPECT_FALSE(result.valid);
	EXPECT_GE(result.errors.size(), 2u);
}

TEST_F(MessageValidatorTest, CombinedRangeRules)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = {
		MessageValidator::required("temp"),
		MessageValidator::type_number("temp"),
		MessageValidator::min_value("temp", -40.0),
		MessageValidator::max_value("temp", 125.0)
	};

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"temp": 36.5})", "test_queue");
	EXPECT_TRUE(result.valid);

	auto result2 = validator_.validate_json(R"({"temp": 200.0})", "test_queue");
	EXPECT_FALSE(result2.valid);

	auto result3 = validator_.validate_json(R"({"temp": -50.0})", "test_queue");
	EXPECT_FALSE(result3.valid);
}

TEST_F(MessageValidatorTest, CombinedStringLengthRules)
{
	MessageSchema schema;
	schema.name = "test";
	schema.description = "test schema";
	schema.rules = {
		MessageValidator::required("name"),
		MessageValidator::type_string("name"),
		MessageValidator::min_length("name", 2),
		MessageValidator::max_length("name", 10)
	};

	validator_.register_schema("test_queue", schema);

	auto result = validator_.validate_json(R"({"name": "sensor"})", "test_queue");
	EXPECT_TRUE(result.valid);

	auto result2 = validator_.validate_json(R"({"name": "x"})", "test_queue");
	EXPECT_FALSE(result2.valid);

	auto result3 = validator_.validate_json(R"({"name": "this-is-a-very-long-name"})", "test_queue");
	EXPECT_FALSE(result3.valid);
}

// =============================================================================
// Schema replacement
// =============================================================================

TEST_F(MessageValidatorTest, RegisterSchemaOverwritesExisting)
{
	MessageSchema schema1;
	schema1.name = "v1";
	schema1.description = "version 1";
	schema1.rules = { MessageValidator::required("field_a") };

	validator_.register_schema("test_queue", schema1);

	auto result1 = validator_.validate_json(R"({"field_b": 1})", "test_queue");
	EXPECT_FALSE(result1.valid);

	MessageSchema schema2;
	schema2.name = "v2";
	schema2.description = "version 2";
	schema2.rules = { MessageValidator::required("field_b") };

	validator_.register_schema("test_queue", schema2);

	auto result2 = validator_.validate_json(R"({"field_b": 1})", "test_queue");
	EXPECT_TRUE(result2.valid);

	auto result3 = validator_.validate_json(R"({"field_a": 1})", "test_queue");
	EXPECT_FALSE(result3.valid);
}
