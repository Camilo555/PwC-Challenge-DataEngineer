"""
GraphQL Router
FastAPI integration for GraphQL endpoint.
"""
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter

from api.graphql.resolvers import schema
from core.logging import get_logger

logger = get_logger(__name__)

# Create GraphQL router with proper configuration
graphql_router = GraphQLRouter(
    schema,
    graphiql=True,  # Enable GraphiQL interface for development
    path="/graphql",
    include_in_schema=True
)

# The router is ready to be included in the main FastAPI app