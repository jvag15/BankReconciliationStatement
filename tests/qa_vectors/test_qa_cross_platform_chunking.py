import pytest
from unittest.mock import patch
from scripts.bulk_ingest import get_dynamic_chunk_size

def test_cross_platform_memory_downscaling_behavior() -> None:
    """Micro-Phase 6.3: Validate cross-platform memory downscaling behavior, ensuring that devices 
    running on limited host specs dynamically step down ingestion chunk sizes via psutil 
    without causing heap allocation crashes.
    """
    # Test Limited Client Node: < 4 GB
    with patch("psutil.virtual_memory") as mock_vm:
        mock_vm.return_value.available = 3 * (1024**3)  # 3 GB
        assert get_dynamic_chunk_size() == 1000

    # Test Mid-Tier Client Node: < 8 GB
    with patch("psutil.virtual_memory") as mock_vm:
        mock_vm.return_value.available = 6 * (1024**3)  # 6 GB
        assert get_dynamic_chunk_size() == 2500

    # Test High-Tier Client Node: >= 8 GB
    with patch("psutil.virtual_memory") as mock_vm:
        mock_vm.return_value.available = 16 * (1024**3)  # 16 GB
        assert get_dynamic_chunk_size() == 5000
