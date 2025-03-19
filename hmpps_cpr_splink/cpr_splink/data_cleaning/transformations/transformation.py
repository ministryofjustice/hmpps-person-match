class Transformation:
    def __init__(self, expression: str):
        self.expression = expression

    def __str__(self):
        return self.expression

    def full_expression(self, *args):
        return self.expression
