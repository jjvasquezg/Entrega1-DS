# Los scripts de usuario deberán definir estas dos funciones:
# def map_func(line: str) -> list[tuple[str, int]]
# def reduce_func(key: str, values: list[int]) -> tuple[str, int]
INTERFACE_DOC = """
Required:
  def map_func(line: str) -> list[tuple[str, bytes|str|int|float]]
  def reduce_func(key: str, values: list) -> tuple[str, bytes|str|int|float]
"""
