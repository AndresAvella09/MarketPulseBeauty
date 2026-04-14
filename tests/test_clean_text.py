import pytest

from src.processing.clean_text import clean_text


@pytest.mark.parametrize(
    "text,expected",
    [
        ("", ""),
        (None, ""),
        ("   ", ""),
    ],
)
def test_clean_text_handles_empty(text, expected):
    assert clean_text(text) == expected


def test_clean_text_removes_urls_and_punctuation():
    text = "Hello!!! Visit https://example.com NOW!!!"
    assert clean_text(text) == "hello visit now"


def test_clean_text_multilingual_input():
    text = "Excelente producto y muy good"
    assert clean_text(text) == "excelente producto muy good"
