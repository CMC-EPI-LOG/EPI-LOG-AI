from app import services


def test_low_risk_advice_skip_policy():
    assert services._should_skip_advice_llm_for_low_risk(
        final_grade="보통",
        age_group="teen_adult",
        user_condition="general",
        temp=22.0,
    ) is True
    assert services._should_skip_advice_llm_for_low_risk(
        final_grade="나쁨",
        age_group="teen_adult",
        user_condition="general",
        temp=22.0,
    ) is False


def test_execution_budget_blocks_second_call():
    budget = services.AdviceExecutionBudget()

    assert budget.consume_embed_call() is True
    assert budget.consume_embed_call() is False
    assert budget.quota_guard_triggered is True


def test_low_risk_clothing_skip_policy():
    assert services._should_skip_clothing_llm_for_low_risk(
        temperature=21.0,
        user_profile={"ageGroup": "teen_adult", "condition": "general"},
        air_quality={"grade": "보통", "pm25Grade": "보통", "pm10Grade": "보통", "o3Grade": "좋음"},
    ) is True
