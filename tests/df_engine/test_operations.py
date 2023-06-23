import numpy as np

from ..fixtures import with_each_df_engine


def get_data_frames(engine):
    return (
        engine.DataFrame([
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [9, 10, 11],
        ]),
        engine.DataFrame.base([
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [9, 10, 11],
        ]),
    )


def get_series(engine):
    return (
        engine.Series([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        engine.Series.base([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
    )


@with_each_df_engine
def test_get_item___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_series_equal(
        wrapped[0],
        unwrapped[0],
    )


@with_each_df_engine
def test_get_item___series(engine):
    wrapped, unwrapped = get_series(engine)

    assert wrapped[0] == unwrapped[0]


@with_each_df_engine
def test_set_item___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    wrapped[0] = 100
    unwrapped[0] = 100

    engine.testing.assert_frame_equal(
        wrapped,
        unwrapped,
    )


@with_each_df_engine
def test_set_item___series(engine):
    wrapped, unwrapped = get_series(engine)

    wrapped[0] = 100
    unwrapped[0] = 100

    engine.testing.assert_series_equal(
        wrapped,
        unwrapped,
    )


@with_each_df_engine
def test_or___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped | 100,
        unwrapped | 100,
    )


@with_each_df_engine
def test_ror___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        100 | wrapped,
        100 | unwrapped,
    )


@with_each_df_engine
def test_ror___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        100 | wrapped,
        100 | unwrapped,
    )


@with_each_df_engine
def test_or___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped | 100,
        unwrapped | 100,
    )


@with_each_df_engine
def test_and___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped & 100,
        unwrapped & 100,
    )


@with_each_df_engine
def test_and___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped & 100,
        unwrapped & 100,
    )


@with_each_df_engine
def test_rand___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        100 & wrapped,
        100 & unwrapped,
    )


@with_each_df_engine
def test_rand___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        100 & wrapped,
        100 & unwrapped,
    )


@with_each_df_engine
def test_xor___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped ^ 100,
        unwrapped ^ 100,
    )


@with_each_df_engine
def test_xor___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped ^ 100,
        unwrapped ^ 100,
    )


@with_each_df_engine
def test_rxor___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        100 ^ wrapped,
        100 ^ unwrapped,
    )


@with_each_df_engine
def test_rxor___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        100 ^ wrapped,
        100 ^ unwrapped,
    )


@with_each_df_engine
def test_invert___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        ~wrapped,
        ~unwrapped,
    )


@with_each_df_engine
def test_invert___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        ~wrapped,
        ~unwrapped,
    )



@with_each_df_engine
def test_neg___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        -wrapped,
        -unwrapped,
    )


@with_each_df_engine
def test_neg___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        -wrapped,
        -unwrapped,
    )


@with_each_df_engine
def test_pos___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        +wrapped,
        +unwrapped,
    )


@with_each_df_engine
def test_add___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped + 1,
        unwrapped + 1,
    )


@with_each_df_engine
def test_add___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped + 1,
        unwrapped + 1,
    )


@with_each_df_engine
def test_radd___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        1 + wrapped,
        1 + unwrapped,
    )


@with_each_df_engine
def test_radd___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        1 + wrapped,
        1 + unwrapped,
    )


@with_each_df_engine
def test_sub___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped - 1,
        unwrapped - 1,
    )


@with_each_df_engine
def test_sub___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped - 1,
        unwrapped - 1,
    )


@with_each_df_engine
def test_rsub___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        1 - wrapped,
        1 - unwrapped,
    )


@with_each_df_engine
def test_rsub___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 - wrapped,
        6 - unwrapped,
    )


@with_each_df_engine
def test_mul___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped * 6,
        unwrapped * 6,
    )


@with_each_df_engine
def test_mul___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped * 6,
        unwrapped * 6,
    )


@with_each_df_engine
def test_rmul___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        6 * wrapped,
        6 * unwrapped,
    )


@with_each_df_engine
def test_rmul___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 * wrapped,
        6 * unwrapped,
    )


@with_each_df_engine
def test_truediv___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped / 6,
        unwrapped / 6,
    )


@with_each_df_engine
def test_truediv___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped / 6,
        unwrapped / 6,
    )


@with_each_df_engine
def test_rtruediv___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        6 / wrapped,
        6 / unwrapped,
    )


@with_each_df_engine
def test_rtruediv___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 / wrapped,
        6 / unwrapped,
    )


@with_each_df_engine
def test_floordiv___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped // 6,
        unwrapped // 6,
    )


@with_each_df_engine
def test_floordiv___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped // 6,
        unwrapped // 6,
    )


@with_each_df_engine
def test_rfloordiv___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        6 // wrapped,
        6 // unwrapped,
    )


@with_each_df_engine
def test_rfloordiv___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 // wrapped,
        6 // unwrapped,
    )


@with_each_df_engine
def test_mod___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped % 6,
        unwrapped % 6,
    )


@with_each_df_engine
def test_mod___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped % 6,
        unwrapped % 6,
    )


@with_each_df_engine
def test_rmod___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        6 % wrapped,
        6 % unwrapped,
    )


@with_each_df_engine
def test_rmod___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 % wrapped,
        6 % unwrapped,
    )


@with_each_df_engine
def test_gt___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped > 6,
        unwrapped > 6,
    )


@with_each_df_engine
def test_gt___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped > 6,
        unwrapped > 6,
    )


@with_each_df_engine
def test_gte___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped >= 6,
        unwrapped >= 6,
    )


@with_each_df_engine
def test_gte___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped >= 6,
        unwrapped >= 6,
    )


@with_each_df_engine
def test_lt___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped < 6,
        unwrapped < 6,
    )


@with_each_df_engine
def test_lt___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped < 6,
        unwrapped < 6,
    )


@with_each_df_engine
def test_lte___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped <= 6,
        unwrapped <= 6,
    )


@with_each_df_engine
def test_lte___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped <= 6,
        unwrapped <= 6,
    )


@with_each_df_engine
def test_eq___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped == 6,
        unwrapped == 6,
    )


@with_each_df_engine
def test_eq___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped == 6,
        unwrapped == 6,
    )


@with_each_df_engine
def test_req___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped == 6,
        unwrapped == 6,
    )


@with_each_df_engine
def test_req___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 == wrapped,
        6 == unwrapped,
    )


@with_each_df_engine
def test_ne___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped != 6,
        unwrapped != 6,
    )


@with_each_df_engine
def test_ne___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        wrapped != 6,
        unwrapped != 6,
    )


@with_each_df_engine
def test_rne___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    engine.testing.assert_frame_equal(
        wrapped != 6,
        unwrapped != 6,
    )


@with_each_df_engine
def test_rne___series(engine):
    wrapped, unwrapped = get_series(engine)

    engine.testing.assert_series_equal(
        6 != wrapped,
        6 != unwrapped,
    )


@with_each_df_engine
def test_contains___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    assert 2 in wrapped


@with_each_df_engine
def test_contains___series(engine):
    wrapped, unwrapped = get_series(engine)

    assert 6 in wrapped


@with_each_df_engine
def test_len___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    assert len(wrapped) == len(unwrapped)


@with_each_df_engine
def test_len___series(engine):
    wrapped, unwrapped = get_series(engine)

    assert len(wrapped) == len(unwrapped)


@with_each_df_engine
def test_iter___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    assert list(wrapped) == list(unwrapped)


@with_each_df_engine
def test_iter___series(engine):
    wrapped, unwrapped = get_series(engine)

    assert list(wrapped) == list(unwrapped)


@with_each_df_engine
def test_array___dataframe(engine):
    wrapped, unwrapped = get_data_frames(engine)

    assert (
        np.array(wrapped) == np.array(unwrapped)
    ).all()


@with_each_df_engine
def test_array___series(engine):
    wrapped, unwrapped = get_series(engine)

    assert (
        np.array(wrapped) == np.array(unwrapped)
    ).all()
