class Transformation:
    def __init__(self, expression: str) -> None:
        self.expression = expression

    def __str__(self) -> str:
        return self.expression

    def full_expression(self, *args):
        return self.expression
