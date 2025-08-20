"""
GraphQL API Module
Provides GraphQL interface for the retail data platform.
"""
from .router import graphql_router
from .schemas import *
from .resolvers import schema

__all__ = ["graphql_router", "schema"]