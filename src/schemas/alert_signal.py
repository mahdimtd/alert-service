from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class AlertSignal(BaseModel):

    model_config = ConfigDict(frozen=True)

    premises_id: int
    rule_id: int
    value: Decimal