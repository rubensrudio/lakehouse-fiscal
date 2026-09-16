from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

_MONTHS = (
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)
_WEEKDAYS = (
    "segunda-feira",
    "terça-feira",
    "quarta-feira",
    "quinta-feira",
    "sexta-feira",
    "sábado",
    "domingo",
)


def _easter(year: int) -> date:
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    month = (
        h
        + (32 + 2 * e + 2 * i - h - k) % 7
        - 7 * ((a + 11 * h + 22 * ((32 + 2 * e + 2 * i - h - k) % 7)) // 451)
        + 114
    ) // 31
    day = (
        (
            h
            + (32 + 2 * e + 2 * i - h - k) % 7
            - 7 * ((a + 11 * h + 22 * ((32 + 2 * e + 2 * i - h - k) % 7)) // 451)
            + 114
        )
        % 31
    ) + 1
    return date(year, month, day)


def build_dim_data(spark: SparkSession) -> DataFrame:
    rows = []
    current = date(2020, 1, 1)
    end = date(2030, 12, 31)
    while current <= end:
        easter = _easter(current.year)
        movable = {
            easter - timedelta(days=48),
            easter - timedelta(days=47),
            easter - timedelta(days=2),
            easter + timedelta(days=60),
        }
        fixed = {(1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25)}
        rows.append(
            (
                int(current.strftime("%Y%m%d")),
                current,
                current.year,
                current.month,
                _MONTHS[current.month - 1],
                current.day,
                current.weekday() + 1,
                _WEEKDAYS[current.weekday()],
                current.weekday() >= 5,
                current in movable or (current.month, current.day) in fixed,
            )
        )
        current += timedelta(days=1)
    return spark.createDataFrame(
        rows,
        "sk_data int, data date, ano int, mes int, nome_mes string, dia int, "
        "dia_semana int, nome_dia_semana string, is_fim_semana boolean, "
        "is_feriado_nacional boolean",
    )
