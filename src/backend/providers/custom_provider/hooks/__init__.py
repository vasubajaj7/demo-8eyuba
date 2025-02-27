# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Package for custom hooks in the Airflow provider.

This package contains custom hooks for various integrations that are not
available in the standard Airflow provider packages. These hooks are
designed to be compatible with Apache Airflow 2.X and Cloud Composer 2.

This initialization file establishes the package structure and enables
proper import paths for hooks contained within the package.
"""

import logging  # Standard logging functionality

# Set up a logger for this module
logger = logging.getLogger(__name__)

# List of modules that should be imported when using
# from airflow.providers.custom_provider.hooks import *
# This list will be populated as hooks are added to the package
__all__ = []