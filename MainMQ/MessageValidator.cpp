#include "MessageValidator.h"

#include <format>
#include <nlohmann/json.hpp>
#include <regex>

using json = nlohmann::json;

auto MessageValidator::register_schema(const std::string& queue, const MessageSchema& schema) -> void
{
	schemas_[queue] = schema;
}

auto MessageValidator::unregister_schema(const std::string& queue) -> void
{
	schemas_.erase(queue);
}

auto MessageValidator::has_schema(const std::string& queue) const -> bool
{
	return schemas_.find(queue) != schemas_.end();
}

auto MessageValidator::get_schema(const std::string& queue) const -> std::optional<MessageSchema>
{
	auto it = schemas_.find(queue);
	if (it != schemas_.end())
	{
		return it->second;
	}
	return std::nullopt;
}

auto MessageValidator::validate(const MessageEnvelope& message) -> ValidationResult
{
	return validate_json(message.payload_json, message.queue);
}

auto MessageValidator::validate_json(const std::string& json_str, const std::string& queue) -> ValidationResult
{
	ValidationResult result;

	auto schema_opt = get_schema(queue);
	if (!schema_opt.has_value())
	{
		return result;
	}

	auto& schema = schema_opt.value();

	json payload;
	try
	{
		payload = json::parse(json_str);
	}
	catch (const json::exception& e)
	{
		result.valid = false;
		result.errors.push_back(std::format("JSON parse error: {}", e.what()));
		return result;
	}

	for (const auto& rule : schema.rules)
	{
		auto error = validate_rule(&payload, rule);
		if (error.has_value())
		{
			result.valid = false;
			result.errors.push_back(error.value());
		}
	}

	return result;
}

auto MessageValidator::validate_rule(const void* json_obj, const ValidationRule& rule) -> std::optional<std::string>
{
	const json& payload = *static_cast<const json*>(json_obj);

	auto make_error = [&](const std::string& default_msg) -> std::string
	{
		return rule.error_message.empty() ? default_msg : rule.error_message;
	};

	switch (rule.type)
	{
	case ValidationRuleType::Required:
	{
		if (!payload.contains(rule.field))
		{
			return make_error(std::format("field '{}' is required", rule.field));
		}
		break;
	}

	case ValidationRuleType::Type:
	{
		if (!payload.contains(rule.field))
		{
			break;
		}

		const auto& value = payload[rule.field];
		bool type_match = false;

		if (rule.expected_type == "string" && value.is_string()) type_match = true;
		else if (rule.expected_type == "number" && value.is_number()) type_match = true;
		else if (rule.expected_type == "boolean" && value.is_boolean()) type_match = true;
		else if (rule.expected_type == "array" && value.is_array()) type_match = true;
		else if (rule.expected_type == "object" && value.is_object()) type_match = true;
		else if (rule.expected_type == "null" && value.is_null()) type_match = true;

		if (!type_match)
		{
			return make_error(std::format("field '{}' must be of type '{}'", rule.field, rule.expected_type));
		}
		break;
	}

	case ValidationRuleType::MinLength:
	{
		if (!payload.contains(rule.field) || !payload[rule.field].is_string())
		{
			break;
		}

		std::string value = payload[rule.field].get<std::string>();
		if (static_cast<int64_t>(value.length()) < rule.min_length)
		{
			return make_error(std::format("field '{}' must be at least {} characters", rule.field, rule.min_length));
		}
		break;
	}

	case ValidationRuleType::MaxLength:
	{
		if (!payload.contains(rule.field) || !payload[rule.field].is_string())
		{
			break;
		}

		std::string value = payload[rule.field].get<std::string>();
		if (static_cast<int64_t>(value.length()) > rule.max_length)
		{
			return make_error(std::format("field '{}' must be at most {} characters", rule.field, rule.max_length));
		}
		break;
	}

	case ValidationRuleType::MinValue:
	{
		if (!payload.contains(rule.field) || !payload[rule.field].is_number())
		{
			break;
		}

		double value = payload[rule.field].get<double>();
		if (value < rule.min_value)
		{
			return make_error(std::format("field '{}' must be at least {}", rule.field, rule.min_value));
		}
		break;
	}

	case ValidationRuleType::MaxValue:
	{
		if (!payload.contains(rule.field) || !payload[rule.field].is_number())
		{
			break;
		}

		double value = payload[rule.field].get<double>();
		if (value > rule.max_value)
		{
			return make_error(std::format("field '{}' must be at most {}", rule.field, rule.max_value));
		}
		break;
	}

	case ValidationRuleType::Pattern:
	{
		if (!payload.contains(rule.field) || !payload[rule.field].is_string())
		{
			break;
		}

		std::string value = payload[rule.field].get<std::string>();
		try
		{
			std::regex pattern(rule.pattern);
			if (!std::regex_match(value, pattern))
			{
				return make_error(std::format("field '{}' does not match pattern '{}'", rule.field, rule.pattern));
			}
		}
		catch (const std::regex_error&)
		{
			return make_error(std::format("invalid regex pattern for field '{}'", rule.field));
		}
		break;
	}

	case ValidationRuleType::Enum:
	{
		if (!payload.contains(rule.field) || !payload[rule.field].is_string())
		{
			break;
		}

		std::string value = payload[rule.field].get<std::string>();
		bool found = false;
		for (const auto& allowed : rule.enum_values)
		{
			if (value == allowed)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			std::string allowed_str;
			for (size_t i = 0; i < rule.enum_values.size(); i++)
			{
				if (i > 0) allowed_str += ", ";
				allowed_str += "'" + rule.enum_values[i] + "'";
			}
			return make_error(std::format("field '{}' must be one of: {}", rule.field, allowed_str));
		}
		break;
	}

	case ValidationRuleType::Custom:
	{
		if (!payload.contains(rule.field))
		{
			break;
		}

		if (!rule.custom_validator)
		{
			return make_error(std::format("field '{}' has unresolved custom validator '{}'", rule.field, rule.custom_validator_name));
		}

		std::string value_str = payload[rule.field].dump();
		if (!rule.custom_validator(value_str))
		{
			return make_error(std::format("field '{}' failed custom validation", rule.field));
		}
		break;
	}
	}

	return std::nullopt;
}

auto MessageValidator::register_custom_validator(const std::string& name, std::function<bool(const std::string&)> validator) -> void
{
	custom_validators_[name] = std::move(validator);
}

auto MessageValidator::has_custom_validator(const std::string& name) const -> bool
{
	return custom_validators_.find(name) != custom_validators_.end();
}

auto MessageValidator::resolve_custom_validators(MessageSchema& schema) -> void
{
	for (auto& rule : schema.rules)
	{
		if (rule.type == ValidationRuleType::Custom && !rule.custom_validator_name.empty() && !rule.custom_validator)
		{
			auto it = custom_validators_.find(rule.custom_validator_name);
			if (it != custom_validators_.end())
			{
				rule.custom_validator = it->second;
			}
		}
	}
}

auto MessageValidator::required(const std::string& field, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Required;
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::type_string(const std::string& field, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Type;
	rule.expected_type = "string";
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::type_number(const std::string& field, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Type;
	rule.expected_type = "number";
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::type_boolean(const std::string& field, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Type;
	rule.expected_type = "boolean";
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::type_array(const std::string& field, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Type;
	rule.expected_type = "array";
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::type_object(const std::string& field, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Type;
	rule.expected_type = "object";
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::min_length(const std::string& field, int64_t min, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::MinLength;
	rule.min_length = min;
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::max_length(const std::string& field, int64_t max, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::MaxLength;
	rule.max_length = max;
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::min_value(const std::string& field, double min, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::MinValue;
	rule.min_value = min;
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::max_value(const std::string& field, double max, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::MaxValue;
	rule.max_value = max;
	rule.error_message = error_msg;
	return rule;
}

auto MessageValidator::enum_values(const std::string& field, const std::vector<std::string>& values, const std::string& error_msg) -> ValidationRule
{
	ValidationRule rule;
	rule.field = field;
	rule.type = ValidationRuleType::Enum;
	rule.enum_values = values;
	rule.error_message = error_msg;
	return rule;
}
