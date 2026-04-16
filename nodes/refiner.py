"""Refiner Node — Self-correction khi SQL fail"""


def refine_sql_node(state: dict) -> dict:
    """Increment retry count and pass error context for regeneration."""
    state["retry_count"] = state.get("retry_count", 0) + 1
    # error_message is already set by validator/executor
    # sql_generator will use error_message for self-correction
    return state
