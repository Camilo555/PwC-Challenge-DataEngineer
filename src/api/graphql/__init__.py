"""
GraphQL API Module
Provides GraphQL interface for the retail data platform.
"""
from .resolvers import schema
from .router import graphql_router
from .schemas import *

__all__ = ["graphql_router", "schema"]
