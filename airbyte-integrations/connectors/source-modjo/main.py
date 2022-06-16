#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_modjo import SourceModjo

if __name__ == "__main__":
    source = SourceModjo()
    launch(source, sys.argv[1:])
