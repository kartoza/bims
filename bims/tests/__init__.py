from unittest import mock

_mock_gsrf = mock.patch(
    'bims.tasks.source_reference.generate_source_reference_filter',
    new=mock.MagicMock(),
)
_mock_gsrf.start()
