"""Test that linking produces expected match stages."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from io_utils import score_similarity


def test_score_similarity_exact():
    assert score_similarity("1 high street london", "1 high street london") == 100


def test_score_similarity_different_numbers():
    """Building number guard: different house numbers should return 0."""
    assert score_similarity("1 high street", "2 high street") == 0


def test_score_similarity_partial():
    score = score_similarity("1 high street london", "1 high street")
    assert 50 <= score <= 100
