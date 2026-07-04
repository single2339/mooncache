use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EligibilityDecision {
    Cacheable,
    Bypass { reason: String },
}

pub fn classify_request(body: &Value, force_replay: bool) -> EligibilityDecision {
    if force_replay {
        return EligibilityDecision::Cacheable;
    }

    let temperature = body
        .get("temperature")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    if temperature == 0.0 {
        EligibilityDecision::Cacheable
    } else {
        EligibilityDecision::Bypass {
            reason: "stochastic temperature requires explicit force replay".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn absent_temperature_is_cacheable() {
        let decision = classify_request(&json!({"model":"gpt-x","input":"hello"}), false);

        assert_eq!(decision, EligibilityDecision::Cacheable);
    }

    #[test]
    fn deterministic_temperature_zero_is_cacheable() {
        let decision = classify_request(
            &json!({"model":"gpt-x","temperature":0,"input":"hello"}),
            false,
        );

        assert_eq!(decision, EligibilityDecision::Cacheable);
    }

    #[test]
    fn stochastic_request_bypasses_without_force_replay() {
        let decision = classify_request(
            &json!({"model":"gpt-x","temperature":0.7,"input":"hello"}),
            false,
        );

        assert!(matches!(decision, EligibilityDecision::Bypass { .. }));
    }

    #[test]
    fn force_replay_allows_stochastic_exact_replay() {
        let decision = classify_request(
            &json!({"model":"gpt-x","temperature":0.7,"input":"hello"}),
            true,
        );

        assert_eq!(decision, EligibilityDecision::Cacheable);
    }
}
