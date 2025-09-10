"""
AI/LLM Conversational Analytics Test Data Fixtures

Comprehensive test data generation for AI conversational analytics testing:
- Natural language query samples
- Conversation context scenarios
- LLM response templates
- Vector embedding mock data
- Multi-turn conversation flows
- Accuracy validation datasets
"""

from __future__ import annotations

import uuid
import json
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class QueryCategory(str, Enum):
    SIMPLE_LOOKUP = "simple_lookup"
    ANALYTICAL = "analytical"
    COMPARATIVE = "comparative"
    PREDICTIVE = "predictive"
    CONTEXTUAL = "contextual"
    COMPLEX_MULTI_STEP = "complex_multi_step"

class LLMProvider(str, Enum):
    OPENAI_GPT4 = "openai-gpt4"
    ANTHROPIC_CLAUDE = "anthropic-claude"
    AZURE_OPENAI = "azure-openai"
    GOOGLE_PALM = "google-palm"

@dataclass
class TestQuery:
    """Structured test query for AI testing"""
    id: str
    category: QueryCategory
    text: str
    intent: str
    expected_entities: List[str]
    difficulty: str
    context_required: bool
    expected_confidence: float

def create_test_nlq_queries() -> List[TestQuery]:
    """Create comprehensive set of test natural language queries"""
    
    queries = [
        # Simple lookup queries
        TestQuery(
            id="simple_001",
            category=QueryCategory.SIMPLE_LOOKUP,
            text="What is our current revenue?",
            intent="revenue_lookup",
            expected_entities=["revenue", "current"],
            difficulty="easy",
            context_required=False,
            expected_confidence=0.95
        ),
        TestQuery(
            id="simple_002", 
            category=QueryCategory.SIMPLE_LOOKUP,
            text="How many orders did we have today?",
            intent="orders_lookup",
            expected_entities=["orders", "today"],
            difficulty="easy",
            context_required=False,
            expected_confidence=0.92
        ),
        TestQuery(
            id="simple_003",
            category=QueryCategory.SIMPLE_LOOKUP,
            text="Show me the conversion rate",
            intent="metrics_lookup",
            expected_entities=["conversion", "rate"],
            difficulty="easy",
            context_required=False,
            expected_confidence=0.90
        ),
        
        # Analytical queries
        TestQuery(
            id="analytical_001",
            category=QueryCategory.ANALYTICAL,
            text="Analyze revenue trends for the last quarter",
            intent="trend_analysis",
            expected_entities=["revenue", "trends", "quarter", "analyze"],
            difficulty="medium",
            context_required=False,
            expected_confidence=0.88
        ),
        TestQuery(
            id="analytical_002",
            category=QueryCategory.ANALYTICAL,
            text="Break down sales performance by region and product category",
            intent="breakdown_analysis",
            expected_entities=["sales", "performance", "region", "product", "category"],
            difficulty="medium",
            context_required=False,
            expected_confidence=0.85
        ),
        TestQuery(
            id="analytical_003",
            category=QueryCategory.ANALYTICAL,
            text="What factors are driving our customer acquisition costs?",
            intent="factor_analysis",
            expected_entities=["factors", "customer", "acquisition", "costs"],
            difficulty="hard",
            context_required=False,
            expected_confidence=0.80
        ),
        
        # Comparative queries
        TestQuery(
            id="comparative_001",
            category=QueryCategory.COMPARATIVE,
            text="Compare Q4 2024 performance vs Q4 2023",
            intent="period_comparison",
            expected_entities=["compare", "Q4", "2024", "2023", "performance"],
            difficulty="medium",
            context_required=False,
            expected_confidence=0.87
        ),
        TestQuery(
            id="comparative_002",
            category=QueryCategory.COMPARATIVE,
            text="How do our conversion rates compare to industry benchmarks?",
            intent="benchmark_comparison",
            expected_entities=["conversion", "rates", "industry", "benchmarks", "compare"],
            difficulty="hard",
            context_required=False,
            expected_confidence=0.75
        ),
        
        # Predictive queries
        TestQuery(
            id="predictive_001",
            category=QueryCategory.PREDICTIVE,
            text="Forecast revenue for Q1 2025 based on current trends",
            intent="revenue_forecast",
            expected_entities=["forecast", "revenue", "Q1", "2025", "trends"],
            difficulty="hard",
            context_required=False,
            expected_confidence=0.78
        ),
        TestQuery(
            id="predictive_002",
            category=QueryCategory.PREDICTIVE,
            text="What will be the impact on sales if we increase marketing spend by 20%?",
            intent="impact_prediction",
            expected_entities=["impact", "sales", "marketing", "spend", "20%"],
            difficulty="expert",
            context_required=False,
            expected_confidence=0.70
        ),
        
        # Contextual queries (require conversation context)
        TestQuery(
            id="contextual_001",
            category=QueryCategory.CONTEXTUAL,
            text="How does that compare to last month?",
            intent="contextual_comparison", 
            expected_entities=["compare", "last", "month"],
            difficulty="medium",
            context_required=True,
            expected_confidence=0.82
        ),
        TestQuery(
            id="contextual_002",
            category=QueryCategory.CONTEXTUAL,
            text="What are the main drivers behind this trend?",
            intent="contextual_analysis",
            expected_entities=["drivers", "trend", "main"],
            difficulty="hard",
            context_required=True,
            expected_confidence=0.75
        ),
        TestQuery(
            id="contextual_003",
            category=QueryCategory.CONTEXTUAL,
            text="Can you break that down by region?",
            intent="contextual_breakdown",
            expected_entities=["break", "down", "region"],
            difficulty="medium",
            context_required=True,
            expected_confidence=0.80
        ),
        
        # Complex multi-step queries
        TestQuery(
            id="complex_001",
            category=QueryCategory.COMPLEX_MULTI_STEP,
            text="Analyze customer retention by segment, identify the lowest performing segment, and suggest improvement strategies",
            intent="complex_analysis_recommendations",
            expected_entities=["analyze", "customer", "retention", "segment", "lowest", "performing", "improvement", "strategies"],
            difficulty="expert",
            context_required=False,
            expected_confidence=0.68
        ),
        TestQuery(
            id="complex_002",
            category=QueryCategory.COMPLEX_MULTI_STEP,
            text="Compare revenue growth across regions, factor in seasonal adjustments, and provide actionable insights for underperforming areas",
            intent="complex_comparative_insights",
            expected_entities=["compare", "revenue", "growth", "regions", "seasonal", "adjustments", "actionable", "insights"],
            difficulty="expert",
            context_required=False,
            expected_confidence=0.65
        )
    ]
    
    return queries

def create_test_conversation_context(
    conversation_type: str = "single_turn",
    turn_count: int = 1
) -> Dict[str, Any]:
    """Create test conversation context for multi-turn testing"""
    
    base_context = {
        "conversation_id": str(uuid.uuid4()),
        "user_id": f"test_user_{uuid.uuid4().hex[:8]}",
        "created_at": datetime.utcnow().isoformat(),
        "last_updated": datetime.utcnow().isoformat(),
        "context_window": [],
        "user_preferences": {
            "analytics_focus": "revenue",
            "visualization_preference": "charts",
            "detail_level": "summary"
        },
        "session_metadata": {
            "platform": "web",
            "user_agent": "test_agent",
            "session_start": datetime.utcnow().isoformat()
        }
    }
    
    if conversation_type == "multi_turn_revenue":
        # Conversation focused on revenue analysis
        turns = [
            {
                "turn": 1,
                "user_query": "Show me Q4 revenue performance",
                "assistant_response": "Q4 revenue reached $2.45M, representing 15.5% growth compared to Q3. This exceeded our target by $200K.",
                "timestamp": (datetime.utcnow() - timedelta(minutes=5)).isoformat(),
                "entities_discussed": ["revenue", "Q4", "growth", "target"]
            },
            {
                "turn": 2,
                "user_query": "How does that compare to last year?",
                "assistant_response": "Compared to Q4 2023, we've seen 23% year-over-year growth. Q4 2023 revenue was $1.99M.",
                "timestamp": (datetime.utcnow() - timedelta(minutes=3)).isoformat(),
                "entities_discussed": ["year-over-year", "Q4 2023", "growth", "comparison"]
            },
            {
                "turn": 3,
                "user_query": "What drove this growth?",
                "assistant_response": "The main growth drivers were: 1) 18% increase in new customer acquisition, 2) 12% improvement in average order value, and 3) successful Black Friday campaign that boosted Q4 sales.",
                "timestamp": (datetime.utcnow() - timedelta(minutes=1)).isoformat(),
                "entities_discussed": ["growth drivers", "customer acquisition", "order value", "campaign"]
            }
        ]
        base_context["context_window"] = turns[:min(turn_count, len(turns))]
        
    elif conversation_type == "multi_turn_comparative":
        # Conversation focused on comparative analysis
        turns = [
            {
                "turn": 1,
                "user_query": "Compare our performance across regions",
                "assistant_response": "Regional performance shows North America leading with $1.2M revenue, followed by Europe ($850K) and Asia-Pacific ($400K). North America grew 25% while Europe grew 12% and APAC grew 8%.",
                "timestamp": (datetime.utcnow() - timedelta(minutes=4)).isoformat(),
                "entities_discussed": ["regions", "North America", "Europe", "Asia-Pacific", "performance"]
            },
            {
                "turn": 2,
                "user_query": "Why is APAC underperforming?",
                "assistant_response": "APAC's slower growth is attributed to: 1) Increased local competition, 2) Economic headwinds in key markets, and 3) Supply chain delays affecting product availability.",
                "timestamp": (datetime.utcnow() - timedelta(minutes=2)).isoformat(),
                "entities_discussed": ["APAC", "underperforming", "competition", "economic", "supply chain"]
            }
        ]
        base_context["context_window"] = turns[:min(turn_count, len(turns))]
        
    elif conversation_type == "context_loss":
        # Simulate context loss scenario
        turns = [
            {
                "turn": 1,
                "user_query": "Show me revenue data", 
                "assistant_response": "Current revenue is $2.45M for Q4.",
                "timestamp": (datetime.utcnow() - timedelta(hours=2)).isoformat(),  # Old timestamp
                "entities_discussed": ["revenue"]
            }
        ]
        base_context["context_window"] = turns
        base_context["context_degraded"] = True
    
    return base_context

def create_test_llm_responses(
    query_category: QueryCategory = QueryCategory.SIMPLE_LOOKUP,
    quality: str = "high"
) -> Dict[str, Any]:
    """Create test LLM responses for different scenarios"""
    
    quality_configs = {
        "high": {
            "confidence": 0.92,
            "accuracy": 0.95,
            "completeness": 0.90,
            "coherence": 0.93
        },
        "medium": {
            "confidence": 0.78,
            "accuracy": 0.82,
            "completeness": 0.75,
            "coherence": 0.80
        },
        "low": {
            "confidence": 0.55,
            "accuracy": 0.60,
            "completeness": 0.50,
            "coherence": 0.58
        }
    }
    
    config = quality_configs.get(quality, quality_configs["high"])
    
    # Category-specific response templates
    response_templates = {
        QueryCategory.SIMPLE_LOOKUP: {
            "high": "Based on current data, the revenue for Q4 is $2.45M, representing a 15.5% increase from Q3. This performance exceeds our quarterly target by $200K.",
            "medium": "Current Q4 revenue is $2.45M with 15.5% growth. Target exceeded by $200K.",
            "low": "Revenue is $2.45M, up 15.5%."
        },
        QueryCategory.ANALYTICAL: {
            "high": "Revenue trend analysis for Q4 shows consistent month-over-month growth: October (+12%), November (+18%), December (+22%). Key drivers include increased customer acquisition (up 25%) and higher average order values (up 8%). The upward trajectory suggests strong market demand and effective sales strategies.",
            "medium": "Q4 revenue trends show positive growth each month. October grew 12%, November 18%, December 22%. Main factors are more customers and higher order values.",
            "low": "Revenue went up each month in Q4. December was best."
        },
        QueryCategory.COMPARATIVE: {
            "high": "Comparing Q4 2024 vs Q4 2023 performance: Revenue increased 23% ($1.99M to $2.45M), orders grew 18% (850 to 1,003), and conversion rate improved 12% (2.1% to 2.35%). This outperformance is attributed to improved marketing efficiency and product-market fit refinements.",
            "medium": "Q4 2024 vs 2023: Revenue up 23% to $2.45M, orders up 18% to 1,003, conversion up 12% to 2.35%.",
            "low": "2024 better than 2023. Revenue up 23%."
        },
        QueryCategory.PREDICTIVE: {
            "high": "Based on current trends and seasonal patterns, Q1 2025 revenue forecast is $2.8M (95% confidence interval: $2.6M-$3.0M). This 14% growth projection assumes continued marketing effectiveness, stable conversion rates, and no major market disruptions. Key risk factors include potential economic slowdown and increased competition.",
            "medium": "Q1 2025 forecast: $2.8M revenue (range $2.6M-$3.0M). Based on current trends and growth patterns.",
            "low": "Next quarter probably $2.8M revenue."
        },
        QueryCategory.CONTEXTUAL: {
            "high": "Comparing to last month's performance, we see a 12% increase in the metric we were discussing. This improvement aligns with the seasonal uptick typically observed in December and suggests our recent optimization efforts are yielding results.",
            "medium": "Compared to last month, we're up 12%. This matches typical December patterns.",
            "low": "Up 12% from last month."
        },
        QueryCategory.COMPLEX_MULTI_STEP: {
            "high": "**Customer Retention Analysis by Segment:**\n\n1. **Segment Performance:**\n   - Premium: 92% retention (excellent)\n   - Standard: 78% retention (good) \n   - Basic: 61% retention (needs improvement)\n\n2. **Lowest Performing Segment:** Basic tier shows concerning 39% churn rate\n\n3. **Root Cause Analysis:**\n   - Limited feature access creates friction\n   - Price sensitivity in economic downturn\n   - Insufficient onboarding support\n\n4. **Improvement Strategies:**\n   - Implement tiered onboarding program\n   - Introduce mid-tier pricing option\n   - Enhanced customer success outreach\n   - Feature usage analytics to identify upgrade opportunities\n\n**Expected Impact:** 15-20% reduction in Basic tier churn within 6 months",
            "medium": "Customer retention by segment: Premium 92%, Standard 78%, Basic 61%. Basic tier needs improvement. Suggest better onboarding, pricing options, and customer success programs. Should reduce churn 15-20%.",
            "low": "Basic customers leave more. Need better support and pricing."
        }
    }
    
    response_text = response_templates[query_category][quality]
    
    return {
        "response_text": response_text,
        "confidence_score": config["confidence"],
        "accuracy_score": config["accuracy"],
        "completeness_score": config["completeness"],
        "coherence_score": config["coherence"],
        "response_time_ms": random.uniform(800, 2500),  # Realistic response times
        "token_count": len(response_text.split()) * 1.3,  # Approximate token count
        "provider_used": random.choice(list(LLMProvider)),
        "generated_at": datetime.utcnow().isoformat(),
        "quality_tier": quality
    }

def create_mock_vector_embeddings(
    text: str,
    dimension: int = 384
) -> List[float]:
    """Create mock vector embeddings for semantic similarity testing"""
    
    # Generate consistent embeddings based on text content
    random.seed(hash(text) % 2147483647)  # Ensure reproducibility
    
    # Create normalized vector
    embedding = [random.gauss(0, 1) for _ in range(dimension)]
    
    # Normalize to unit vector
    magnitude = sum(x**2 for x in embedding) ** 0.5
    if magnitude > 0:
        embedding = [x / magnitude for x in embedding]
    
    return embedding

def create_test_streaming_chunks(
    response_text: str,
    chunk_count: int = 6
) -> List[Dict[str, Any]]:
    """Create test streaming response chunks"""
    
    # Split response into logical chunks
    sentences = response_text.split('. ')
    chunks_per_sentence = max(1, chunk_count // len(sentences))
    
    chunks = []
    chunk_id = 0
    
    # Text chunks
    for i, sentence in enumerate(sentences):
        chunks.append({
            "chunk_id": str(uuid.uuid4()),
            "sequence_id": chunk_id,
            "chunk_type": "text",
            "content": {"text": sentence + ("." if i < len(sentences) - 1 else "")},
            "timestamp": datetime.utcnow().isoformat(),
            "is_final": False
        })
        chunk_id += 1
    
    # Data chunk (if analytical response)
    if any(keyword in response_text.lower() for keyword in ["revenue", "growth", "analysis"]):
        chunks.insert(-1, {
            "chunk_id": str(uuid.uuid4()),
            "sequence_id": chunk_id,
            "chunk_type": "data",
            "content": {
                "metrics": {"revenue": 2450000.50, "growth": 15.5},
                "time_period": "Q4 2024"
            },
            "timestamp": datetime.utcnow().isoformat(),
            "is_final": False
        })
        chunk_id += 1
    
    # Insight chunk
    chunks.insert(-1, {
        "chunk_id": str(uuid.uuid4()),
        "sequence_id": chunk_id,
        "chunk_type": "insight",
        "content": {
            "insight_type": "trend",
            "confidence": 0.92,
            "key_finding": "Strong upward revenue trend"
        },
        "timestamp": datetime.utcnow().isoformat(),
        "is_final": False
    })
    chunk_id += 1
    
    # Final completion chunk
    chunks.append({
        "chunk_id": str(uuid.uuid4()),
        "sequence_id": chunk_id,
        "chunk_type": "complete",
        "content": {"status": "completed", "total_chunks": len(chunks)},
        "timestamp": datetime.utcnow().isoformat(),
        "is_final": True
    })
    
    return chunks

def create_test_accuracy_ground_truth() -> Dict[str, Dict[str, Any]]:
    """Create ground truth data for accuracy testing"""
    
    return {
        "mathematical_queries": {
            "growth_calculation": {
                "query": "If Q4 revenue was $2.5M and Q3 was $2.0M, what is the percentage growth?",
                "correct_answer": "25%",
                "calculation": "(2.5 - 2.0) / 2.0 * 100 = 25%",
                "expected_elements": ["25%", "25 percent", "growth"],
                "accuracy_threshold": 0.98
            },
            "compound_growth": {
                "query": "Calculate CAGR from $1M to $2.5M over 3 years",
                "correct_answer": "35.7%",
                "calculation": "((2.5/1.0)^(1/3)) - 1 = 0.357",
                "expected_elements": ["35.7%", "CAGR", "compound"],
                "accuracy_threshold": 0.95
            }
        },
        "factual_queries": {
            "roi_formula": {
                "query": "What is the standard ROI formula?",
                "correct_answer": "ROI = (Gain from Investment - Cost of Investment) / Cost of Investment * 100",
                "expected_elements": ["ROI", "Gain", "Cost", "Investment", "formula"],
                "accuracy_threshold": 0.90
            },
            "customer_lifetime_value": {
                "query": "How do you calculate customer lifetime value?",
                "correct_answer": "CLV = (Average Revenue per Customer * Customer Lifespan) - Acquisition Cost",
                "expected_elements": ["CLV", "Average Revenue", "Customer Lifespan", "Acquisition Cost"],
                "accuracy_threshold": 0.85
            }
        },
        "business_logic_queries": {
            "retention_factors": {
                "query": "What factors affect customer retention?",
                "correct_elements": [
                    "customer service", "product quality", "pricing", "user experience",
                    "satisfaction", "loyalty programs", "competition", "value proposition"
                ],
                "accuracy_threshold": 0.80
            },
            "performance_metrics": {
                "query": "What are key business health metrics?",
                "correct_elements": [
                    "revenue", "profit margins", "cash flow", "customer acquisition",
                    "retention rate", "growth rate", "market share", "operational efficiency"
                ],
                "accuracy_threshold": 0.75
            }
        }
    }

def create_test_conversation_flows() -> List[Dict[str, Any]]:
    """Create test conversation flows for flow testing"""
    
    flows = [
        {
            "flow_id": "revenue_deep_dive",
            "description": "Deep dive into revenue analysis",
            "turns": [
                {
                    "turn": 1,
                    "user_input": "Show me Q4 revenue performance",
                    "expected_intent": "revenue_analysis",
                    "expected_entities": ["Q4", "revenue", "performance"],
                    "context_updates": ["current_topic: revenue", "time_period: Q4"]
                },
                {
                    "turn": 2,
                    "user_input": "How does that compare to Q3?",
                    "expected_intent": "contextual_comparison",
                    "expected_entities": ["compare", "Q3"],
                    "context_required": ["current_topic", "time_period"]
                },
                {
                    "turn": 3,
                    "user_input": "What drove the growth?",
                    "expected_intent": "factor_analysis", 
                    "expected_entities": ["drove", "growth"],
                    "context_required": ["current_topic", "comparison_results"]
                }
            ],
            "success_criteria": {
                "context_retention": True,
                "intent_accuracy": 0.90,
                "response_coherence": 0.85
            }
        },
        {
            "flow_id": "segmentation_analysis",
            "description": "Customer segmentation analysis flow",
            "turns": [
                {
                    "turn": 1,
                    "user_input": "Analyze customer segments performance",
                    "expected_intent": "segmentation_analysis",
                    "expected_entities": ["customer", "segments", "performance"],
                    "context_updates": ["analysis_type: segmentation", "focus: customers"]
                },
                {
                    "turn": 2,
                    "user_input": "Which segment is underperforming?",
                    "expected_intent": "segment_identification",
                    "expected_entities": ["segment", "underperforming"],
                    "context_required": ["analysis_type", "focus"]
                },
                {
                    "turn": 3,
                    "user_input": "What can we do to improve it?",
                    "expected_intent": "improvement_recommendations",
                    "expected_entities": ["improve", "recommendations"],
                    "context_required": ["underperforming_segment"]
                }
            ],
            "success_criteria": {
                "context_retention": True,
                "intent_accuracy": 0.85,
                "recommendation_quality": 0.80
            }
        }
    ]
    
    return flows

def create_test_provider_configurations() -> Dict[LLMProvider, Dict[str, Any]]:
    """Create test configurations for different LLM providers"""
    
    return {
        LLMProvider.OPENAI_GPT4: {
            "model": "gpt-4",
            "max_tokens": 4000,
            "temperature": 0.1,
            "reliability_score": 0.98,
            "avg_response_time_ms": 1500,
            "cost_per_token": 0.00003,
            "capabilities": ["text", "analysis", "reasoning", "math"],
            "test_response_quality": "high"
        },
        LLMProvider.ANTHROPIC_CLAUDE: {
            "model": "claude-3-sonnet",
            "max_tokens": 4000,
            "temperature": 0.1,
            "reliability_score": 0.97,
            "avg_response_time_ms": 1200,
            "cost_per_token": 0.000025,
            "capabilities": ["text", "analysis", "reasoning", "coding"],
            "test_response_quality": "high"
        },
        LLMProvider.AZURE_OPENAI: {
            "model": "gpt-35-turbo",
            "max_tokens": 3000,
            "temperature": 0.1,
            "reliability_score": 0.96,
            "avg_response_time_ms": 1800,
            "cost_per_token": 0.000002,
            "capabilities": ["text", "analysis", "basic_reasoning"],
            "test_response_quality": "medium"
        },
        LLMProvider.GOOGLE_PALM: {
            "model": "palm-2",
            "max_tokens": 2048,
            "temperature": 0.1,
            "reliability_score": 0.94,
            "avg_response_time_ms": 2200,
            "cost_per_token": 0.000001,
            "capabilities": ["text", "basic_analysis"],
            "test_response_quality": "medium"
        }
    }

# Export key functions for use in tests
__all__ = [
    "create_test_nlq_queries",
    "create_test_conversation_context",
    "create_test_llm_responses",
    "create_mock_vector_embeddings", 
    "create_test_streaming_chunks",
    "create_test_accuracy_ground_truth",
    "create_test_conversation_flows",
    "create_test_provider_configurations",
    "QueryCategory",
    "LLMProvider",
    "TestQuery"
]