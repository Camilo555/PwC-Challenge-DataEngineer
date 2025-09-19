"""
Responsive Mobile Dashboard Components
=====================================

Mobile-optimized dashboard components with responsive design and touch optimization.
Supporting the $2.8M mobile analytics platform opportunity.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from pydantic import BaseModel, Field


class ResponsiveBreakpoint(str, Enum):
    """Responsive design breakpoints."""
    MOBILE_SMALL = "xs"    # < 480px
    MOBILE_LARGE = "sm"    # 480px - 767px
    TABLET = "md"          # 768px - 1023px
    DESKTOP = "lg"         # 1024px - 1199px
    DESKTOP_LARGE = "xl"   # >= 1200px


class TouchGesture(str, Enum):
    """Supported touch gestures."""
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE_LEFT = "swipe_left"
    SWIPE_RIGHT = "swipe_right"
    SWIPE_UP = "swipe_up"
    SWIPE_DOWN = "swipe_down"
    PINCH_ZOOM = "pinch_zoom"
    ROTATE = "rotate"


class MobileWidgetConfig(BaseModel):
    """Configuration for mobile-optimized widgets."""
    widget_id: str
    widget_type: str
    title: str
    responsive_config: Dict[ResponsiveBreakpoint, Dict[str, Any]]
    touch_gestures: List[TouchGesture] = Field(default_factory=list)
    offline_capable: bool = False
    cache_duration_minutes: int = 15
    lazy_load: bool = True
    performance_priority: int = Field(default=5, ge=1, le=10)


class ResponsiveMobileDashboard:
    """Responsive mobile dashboard with adaptive layouts and touch optimization."""

    def __init__(self):
        """Initialize responsive mobile dashboard."""
        self.breakpoints = {
            ResponsiveBreakpoint.MOBILE_SMALL: {"max_width": 479, "columns": 1, "padding": 8},
            ResponsiveBreakpoint.MOBILE_LARGE: {"max_width": 767, "columns": 2, "padding": 12},
            ResponsiveBreakpoint.TABLET: {"max_width": 1023, "columns": 3, "padding": 16},
            ResponsiveBreakpoint.DESKTOP: {"max_width": 1199, "columns": 4, "padding": 20},
            ResponsiveBreakpoint.DESKTOP_LARGE: {"max_width": 9999, "columns": 6, "padding": 24}
        }

        self.widget_templates = self._create_mobile_widget_templates()

    def _create_mobile_widget_templates(self) -> Dict[str, MobileWidgetConfig]:
        """Create mobile-optimized widget templates."""
        return {
            "mobile_kpi_card": MobileWidgetConfig(
                widget_id="mobile_kpi_card",
                widget_type="kpi_card",
                title="KPI Card",
                responsive_config={
                    ResponsiveBreakpoint.MOBILE_SMALL: {
                        "width": "100%",
                        "height": "120px",
                        "font_size": "14px",
                        "padding": "12px",
                        "layout": "vertical"
                    },
                    ResponsiveBreakpoint.MOBILE_LARGE: {
                        "width": "48%",
                        "height": "140px",
                        "font_size": "16px",
                        "padding": "16px",
                        "layout": "vertical"
                    },
                    ResponsiveBreakpoint.TABLET: {
                        "width": "32%",
                        "height": "160px",
                        "font_size": "18px",
                        "padding": "20px",
                        "layout": "horizontal"
                    }
                },
                touch_gestures=[TouchGesture.TAP, TouchGesture.LONG_PRESS],
                offline_capable=True,
                cache_duration_minutes=30,
                performance_priority=9
            ),

            "mobile_chart": MobileWidgetConfig(
                widget_id="mobile_chart",
                widget_type="chart",
                title="Mobile Chart",
                responsive_config={
                    ResponsiveBreakpoint.MOBILE_SMALL: {
                        "width": "100%",
                        "height": "250px",
                        "chart_type": "line",
                        "max_data_points": 20,
                        "show_legend": False,
                        "show_labels": False,
                        "touch_zoom": True
                    },
                    ResponsiveBreakpoint.MOBILE_LARGE: {
                        "width": "100%",
                        "height": "300px",
                        "chart_type": "line",
                        "max_data_points": 40,
                        "show_legend": True,
                        "show_labels": True,
                        "touch_zoom": True
                    },
                    ResponsiveBreakpoint.TABLET: {
                        "width": "100%",
                        "height": "400px",
                        "chart_type": "line",
                        "max_data_points": 100,
                        "show_legend": True,
                        "show_labels": True,
                        "touch_zoom": True,
                        "interactive_tooltip": True
                    }
                },
                touch_gestures=[TouchGesture.TAP, TouchGesture.PINCH_ZOOM, TouchGesture.SWIPE_LEFT, TouchGesture.SWIPE_RIGHT],
                offline_capable=True,
                cache_duration_minutes=10,
                performance_priority=7
            ),

            "mobile_data_table": MobileWidgetConfig(
                widget_id="mobile_data_table",
                widget_type="data_table",
                title="Data Table",
                responsive_config={
                    ResponsiveBreakpoint.MOBILE_SMALL: {
                        "width": "100%",
                        "height": "300px",
                        "layout": "card_list",  # Convert table to card layout
                        "max_rows": 10,
                        "columns": ["name", "value"],
                        "horizontal_scroll": False,
                        "pagination": True
                    },
                    ResponsiveBreakpoint.MOBILE_LARGE: {
                        "width": "100%",
                        "height": "350px",
                        "layout": "compact_table",
                        "max_rows": 15,
                        "columns": ["name", "value", "change"],
                        "horizontal_scroll": True,
                        "pagination": True
                    },
                    ResponsiveBreakpoint.TABLET: {
                        "width": "100%",
                        "height": "400px",
                        "layout": "full_table",
                        "max_rows": 20,
                        "columns": ["name", "value", "change", "percentage"],
                        "horizontal_scroll": False,
                        "pagination": True,
                        "sortable": True,
                        "filterable": True
                    }
                },
                touch_gestures=[TouchGesture.TAP, TouchGesture.SWIPE_UP, TouchGesture.SWIPE_DOWN],
                offline_capable=False,
                cache_duration_minutes=5,
                performance_priority=6
            ),

            "mobile_filter_panel": MobileWidgetConfig(
                widget_id="mobile_filter_panel",
                widget_type="filter_panel",
                title="Filters",
                responsive_config={
                    ResponsiveBreakpoint.MOBILE_SMALL: {
                        "width": "100%",
                        "height": "auto",
                        "layout": "collapsible",
                        "position": "top",
                        "filter_style": "dropdown",
                        "max_visible_filters": 2
                    },
                    ResponsiveBreakpoint.MOBILE_LARGE: {
                        "width": "100%",
                        "height": "auto",
                        "layout": "horizontal",
                        "position": "top",
                        "filter_style": "button_group",
                        "max_visible_filters": 3
                    },
                    ResponsiveBreakpoint.TABLET: {
                        "width": "250px",
                        "height": "auto",
                        "layout": "sidebar",
                        "position": "left",
                        "filter_style": "checkbox_list",
                        "max_visible_filters": 6
                    }
                },
                touch_gestures=[TouchGesture.TAP, TouchGesture.SWIPE_UP, TouchGesture.SWIPE_DOWN],
                offline_capable=True,
                cache_duration_minutes=60,
                performance_priority=8
            ),

            "mobile_metric_summary": MobileWidgetConfig(
                widget_id="mobile_metric_summary",
                widget_type="metric_summary",
                title="Metrics Summary",
                responsive_config={
                    ResponsiveBreakpoint.MOBILE_SMALL: {
                        "width": "100%",
                        "height": "180px",
                        "layout": "vertical_stack",
                        "metrics_count": 3,
                        "show_sparklines": False,
                        "show_trends": True,
                        "compact_mode": True
                    },
                    ResponsiveBreakpoint.MOBILE_LARGE: {
                        "width": "100%",
                        "height": "200px",
                        "layout": "horizontal_grid",
                        "metrics_count": 4,
                        "show_sparklines": True,
                        "show_trends": True,
                        "compact_mode": False
                    },
                    ResponsiveBreakpoint.TABLET: {
                        "width": "100%",
                        "height": "220px",
                        "layout": "grid_2x3",
                        "metrics_count": 6,
                        "show_sparklines": True,
                        "show_trends": True,
                        "show_comparisons": True,
                        "compact_mode": False
                    }
                },
                touch_gestures=[TouchGesture.TAP],
                offline_capable=True,
                cache_duration_minutes=15,
                performance_priority=10
            )
        }

    def generate_responsive_layout(
        self,
        screen_width: int,
        screen_height: int,
        widgets: List[str],
        user_preferences: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Generate responsive layout based on screen dimensions."""

        # Determine current breakpoint
        current_breakpoint = self._get_breakpoint_for_width(screen_width)
        breakpoint_config = self.breakpoints[current_breakpoint]

        # Create base layout configuration
        layout_config = {
            "breakpoint": current_breakpoint.value,
            "screen_dimensions": {"width": screen_width, "height": screen_height},
            "grid_config": {
                "columns": breakpoint_config["columns"],
                "padding": breakpoint_config["padding"],
                "gap": 12 if current_breakpoint in [ResponsiveBreakpoint.MOBILE_SMALL, ResponsiveBreakpoint.MOBILE_LARGE] else 16
            },
            "widgets": [],
            "touch_optimizations": self._get_touch_optimizations(current_breakpoint),
            "performance_config": self._get_performance_config(current_breakpoint)
        }

        # Configure each widget for current breakpoint
        for widget_name in widgets:
            if widget_name in self.widget_templates:
                widget_template = self.widget_templates[widget_name]
                widget_config = self._configure_widget_for_breakpoint(
                    widget_template,
                    current_breakpoint,
                    user_preferences
                )
                layout_config["widgets"].append(widget_config)

        # Apply mobile-specific optimizations
        if current_breakpoint in [ResponsiveBreakpoint.MOBILE_SMALL, ResponsiveBreakpoint.MOBILE_LARGE]:
            layout_config = self._apply_mobile_optimizations(layout_config)

        return layout_config

    def _get_breakpoint_for_width(self, width: int) -> ResponsiveBreakpoint:
        """Determine responsive breakpoint based on screen width."""
        for breakpoint, config in self.breakpoints.items():
            if width <= config["max_width"]:
                return breakpoint
        return ResponsiveBreakpoint.DESKTOP_LARGE

    def _configure_widget_for_breakpoint(
        self,
        widget_template: MobileWidgetConfig,
        breakpoint: ResponsiveBreakpoint,
        user_preferences: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Configure widget for specific breakpoint."""

        # Get base configuration for breakpoint
        base_config = widget_template.responsive_config.get(
            breakpoint,
            widget_template.responsive_config[ResponsiveBreakpoint.MOBILE_SMALL]
        )

        widget_config = {
            "widget_id": widget_template.widget_id,
            "widget_type": widget_template.widget_type,
            "title": widget_template.title,
            "config": base_config.copy(),
            "touch_gestures": widget_template.touch_gestures,
            "offline_capable": widget_template.offline_capable,
            "cache_duration_minutes": widget_template.cache_duration_minutes,
            "lazy_load": widget_template.lazy_load,
            "performance_priority": widget_template.performance_priority
        }

        # Apply user preferences if provided
        if user_preferences:
            widget_config = self._apply_user_preferences(widget_config, user_preferences)

        # Add accessibility enhancements
        widget_config["accessibility"] = self._get_accessibility_config(breakpoint)

        return widget_config

    def _apply_mobile_optimizations(self, layout_config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply mobile-specific optimizations to layout."""

        # Sort widgets by performance priority
        layout_config["widgets"].sort(key=lambda w: w["performance_priority"], reverse=True)

        # Enable aggressive caching for mobile
        layout_config["caching"] = {
            "strategy": "aggressive",
            "service_worker_enabled": True,
            "offline_fallbacks": True,
            "cache_api_responses": True
        }

        # Add mobile navigation optimizations
        layout_config["navigation"] = {
            "style": "bottom_tab" if layout_config["breakpoint"] in ["xs", "sm"] else "sidebar",
            "swipe_navigation": True,
            "back_gesture": True,
            "pull_to_refresh": True
        }

        # Add touch target optimizations
        layout_config["touch_targets"] = {
            "minimum_size": 44,  # 44px minimum touch target
            "spacing": 8,        # 8px minimum spacing
            "feedback_enabled": True,
            "haptic_feedback": True
        }

        # Add performance optimizations
        layout_config["performance"] = {
            "lazy_loading": True,
            "virtual_scrolling": True,
            "image_optimization": True,
            "bundle_splitting": True,
            "preload_critical": True
        }

        return layout_config

    def _get_touch_optimizations(self, breakpoint: ResponsiveBreakpoint) -> Dict[str, Any]:
        """Get touch optimizations for breakpoint."""
        is_touch_device = breakpoint in [
            ResponsiveBreakpoint.MOBILE_SMALL,
            ResponsiveBreakpoint.MOBILE_LARGE,
            ResponsiveBreakpoint.TABLET
        ]

        return {
            "enabled": is_touch_device,
            "gesture_recognition": is_touch_device,
            "touch_feedback": is_touch_device,
            "scroll_momentum": is_touch_device,
            "pinch_zoom": breakpoint != ResponsiveBreakpoint.MOBILE_SMALL,
            "double_tap_zoom": is_touch_device,
            "swipe_navigation": breakpoint in [ResponsiveBreakpoint.MOBILE_SMALL, ResponsiveBreakpoint.MOBILE_LARGE]
        }

    def _get_performance_config(self, breakpoint: ResponsiveBreakpoint) -> Dict[str, Any]:
        """Get performance configuration for breakpoint."""
        is_mobile = breakpoint in [ResponsiveBreakpoint.MOBILE_SMALL, ResponsiveBreakpoint.MOBILE_LARGE]

        return {
            "lazy_loading_enabled": True,
            "image_compression": is_mobile,
            "bundle_splitting": is_mobile,
            "service_worker": is_mobile,
            "cache_strategy": "aggressive" if is_mobile else "standard",
            "preload_threshold": 3 if is_mobile else 5,
            "animation_reduced": is_mobile,
            "network_aware": is_mobile
        }

    def _apply_user_preferences(
        self,
        widget_config: Dict[str, Any],
        user_preferences: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply user preferences to widget configuration."""

        # Apply theme preferences
        if "theme" in user_preferences:
            widget_config["theme"] = user_preferences["theme"]

        # Apply font size preferences
        if "font_scale" in user_preferences:
            scale = user_preferences["font_scale"]
            if "font_size" in widget_config["config"]:
                base_size = int(widget_config["config"]["font_size"].replace("px", ""))
                widget_config["config"]["font_size"] = f"{int(base_size * scale)}px"

        # Apply accessibility preferences
        if "high_contrast" in user_preferences and user_preferences["high_contrast"]:
            widget_config["accessibility"]["high_contrast"] = True

        # Apply animation preferences
        if "reduce_motion" in user_preferences and user_preferences["reduce_motion"]:
            widget_config["config"]["animations_enabled"] = False

        return widget_config

    def _get_accessibility_config(self, breakpoint: ResponsiveBreakpoint) -> Dict[str, Any]:
        """Get accessibility configuration for breakpoint."""
        return {
            "aria_labels": True,
            "keyboard_navigation": breakpoint in [ResponsiveBreakpoint.DESKTOP, ResponsiveBreakpoint.DESKTOP_LARGE],
            "screen_reader_support": True,
            "high_contrast_support": True,
            "focus_indicators": True,
            "semantic_markup": True,
            "alt_text_required": True,
            "color_blind_friendly": True,
            "touch_target_guidelines": breakpoint in [
                ResponsiveBreakpoint.MOBILE_SMALL,
                ResponsiveBreakpoint.MOBILE_LARGE,
                ResponsiveBreakpoint.TABLET
            ]
        }

    def generate_mobile_css(self, layout_config: Dict[str, Any]) -> str:
        """Generate responsive CSS for mobile dashboard."""

        css_rules = []

        # Base mobile styles
        css_rules.append("""
/* Base Mobile Dashboard Styles */
.mobile-dashboard {
    width: 100%;
    padding: 0;
    margin: 0;
    box-sizing: border-box;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* Touch optimizations */
.touch-target {
    min-height: 44px;
    min-width: 44px;
    cursor: pointer;
    -webkit-tap-highlight-color: rgba(0, 0, 0, 0.1);
}

.touch-target:active {
    transform: scale(0.98);
    transition: transform 0.1s ease;
}

/* Responsive grid */
.dashboard-grid {
    display: grid;
    gap: 12px;
    padding: 16px;
}
""")

        # Breakpoint-specific styles
        breakpoint = layout_config["breakpoint"]
        grid_columns = layout_config["grid_config"]["columns"]

        if breakpoint == "xs":
            css_rules.append(f"""
/* Mobile Small (< 480px) */
@media (max-width: 479px) {{
    .dashboard-grid {{
        grid-template-columns: 1fr;
        padding: 8px;
        gap: 8px;
    }}

    .mobile-widget {{
        width: 100%;
        margin-bottom: 12px;
    }}

    .mobile-chart {{
        height: 250px;
    }}

    .mobile-kpi-card {{
        height: 120px;
        padding: 12px;
    }}

    .mobile-table {{
        display: none; /* Hide tables on very small screens */
    }}

    .mobile-table-cards {{
        display: block;
    }}
}}
""")

        elif breakpoint == "sm":
            css_rules.append(f"""
/* Mobile Large (480px - 767px) */
@media (min-width: 480px) and (max-width: 767px) {{
    .dashboard-grid {{
        grid-template-columns: repeat(2, 1fr);
        padding: 12px;
        gap: 12px;
    }}

    .mobile-widget.full-width {{
        grid-column: 1 / -1;
    }}

    .mobile-chart {{
        height: 300px;
        grid-column: 1 / -1;
    }}

    .mobile-kpi-card {{
        height: 140px;
        padding: 16px;
    }}
}}
""")

        elif breakpoint == "md":
            css_rules.append(f"""
/* Tablet (768px - 1023px) */
@media (min-width: 768px) and (max-width: 1023px) {{
    .dashboard-grid {{
        grid-template-columns: repeat(3, 1fr);
        padding: 16px;
        gap: 16px;
    }}

    .mobile-chart {{
        height: 400px;
    }}

    .mobile-kpi-card {{
        height: 160px;
        padding: 20px;
    }}

    .mobile-table {{
        display: table;
        width: 100%;
    }}

    .mobile-table-cards {{
        display: none;
    }}
}}
""")

        # Widget-specific styles
        for widget in layout_config["widgets"]:
            widget_id = widget["widget_id"]
            widget_config = widget["config"]

            css_rules.append(f"""
/* {widget_id} Styles */
.{widget_id} {{
    background: white;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    overflow: hidden;
    transition: box-shadow 0.2s ease;
}}

.{widget_id}:hover {{
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
}}
""")

        # Touch gesture styles
        if layout_config.get("touch_optimizations", {}).get("enabled"):
            css_rules.append("""
/* Touch gesture styles */
.swipeable {
    touch-action: pan-x;
    -webkit-overflow-scrolling: touch;
}

.pinch-zoomable {
    touch-action: manipulation;
}

.scrollable {
    -webkit-overflow-scrolling: touch;
    overscroll-behavior: contain;
}

/* Haptic feedback simulation */
.haptic-light:active {
    animation: haptic-light 0.1s ease;
}

@keyframes haptic-light {
    0% { transform: scale(1); }
    50% { transform: scale(0.98); }
    100% { transform: scale(1); }
}
""")

        # Accessibility styles
        css_rules.append("""
/* Accessibility styles */
.high-contrast {
    filter: contrast(150%);
}

.reduced-motion {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
}

.focus-visible {
    outline: 2px solid #0066cc;
    outline-offset: 2px;
}

/* Screen reader only content */
.sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
}
""")

        return "\n".join(css_rules)

    def generate_mobile_javascript(self, layout_config: Dict[str, Any]) -> str:
        """Generate JavaScript for mobile dashboard interactions."""

        js_code = """
// Mobile Dashboard JavaScript
class MobileDashboard {
    constructor(config) {
        this.config = config;
        this.touchStartX = 0;
        this.touchStartY = 0;
        this.currentBreakpoint = config.breakpoint;
        this.init();
    }

    init() {
        this.setupTouchHandlers();
        this.setupResizeHandler();
        this.setupServiceWorker();
        this.initializeWidgets();
    }

    setupTouchHandlers() {
        document.addEventListener('touchstart', (e) => {
            this.touchStartX = e.touches[0].clientX;
            this.touchStartY = e.touches[0].clientY;
        }, { passive: true });

        document.addEventListener('touchend', (e) => {
            this.handleTouchEnd(e);
        }, { passive: true });

        // Setup pinch zoom for charts
        document.querySelectorAll('.pinch-zoomable').forEach(element => {
            let scale = 1;
            let lastDistance = 0;

            element.addEventListener('touchmove', (e) => {
                if (e.touches.length === 2) {
                    e.preventDefault();
                    const distance = this.getDistance(e.touches[0], e.touches[1]);

                    if (lastDistance > 0) {
                        scale *= distance / lastDistance;
                        scale = Math.min(Math.max(0.5, scale), 3);
                        element.style.transform = `scale(${scale})`;
                    }
                    lastDistance = distance;
                }
            });
        });
    }

    handleTouchEnd(e) {
        const touchEndX = e.changedTouches[0].clientX;
        const touchEndY = e.changedTouches[0].clientY;
        const deltaX = touchEndX - this.touchStartX;
        const deltaY = touchEndY - this.touchStartY;

        // Detect swipe gestures
        if (Math.abs(deltaX) > Math.abs(deltaY)) {
            if (Math.abs(deltaX) > 50) {
                if (deltaX > 0) {
                    this.handleSwipeRight();
                } else {
                    this.handleSwipeLeft();
                }
            }
        } else {
            if (Math.abs(deltaY) > 50) {
                if (deltaY > 0) {
                    this.handleSwipeDown();
                } else {
                    this.handleSwipeUp();
                }
            }
        }
    }

    getDistance(touch1, touch2) {
        return Math.sqrt(
            Math.pow(touch2.clientX - touch1.clientX, 2) +
            Math.pow(touch2.clientY - touch1.clientY, 2)
        );
    }

    handleSwipeLeft() {
        // Navigate to next dashboard section
        this.navigateNext();
    }

    handleSwipeRight() {
        // Navigate to previous dashboard section
        this.navigatePrevious();
    }

    handleSwipeUp() {
        // Refresh dashboard data
        this.refreshData();
    }

    handleSwipeDown() {
        // Show filters or additional options
        this.toggleFilters();
    }

    setupResizeHandler() {
        let resizeTimer;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(() => {
                this.handleResize();
            }, 250);
        });
    }

    handleResize() {
        const newBreakpoint = this.getBreakpointForWidth(window.innerWidth);
        if (newBreakpoint !== this.currentBreakpoint) {
            this.currentBreakpoint = newBreakpoint;
            this.reconfigureLayout();
        }
    }

    getBreakpointForWidth(width) {
        if (width < 480) return 'xs';
        if (width < 768) return 'sm';
        if (width < 1024) return 'md';
        if (width < 1200) return 'lg';
        return 'xl';
    }

    setupServiceWorker() {
        if ('serviceWorker' in navigator) {
            navigator.serviceWorker.register('/dashboard-sw.js')
                .then(registration => {
                    console.log('Dashboard SW registered:', registration);
                })
                .catch(error => {
                    console.log('Dashboard SW registration failed:', error);
                });
        }
    }

    initializeWidgets() {
        this.config.widgets.forEach(widget => {
            this.initializeWidget(widget);
        });
    }

    initializeWidget(widget) {
        const element = document.querySelector(`#${widget.widget_id}`);
        if (!element) return;

        // Setup lazy loading
        if (widget.lazy_load) {
            this.setupLazyLoading(element);
        }

        // Setup touch gestures
        widget.touch_gestures.forEach(gesture => {
            this.setupGesture(element, gesture);
        });

        // Setup offline capability
        if (widget.offline_capable) {
            this.setupOfflineSupport(element, widget);
        }
    }

    setupLazyLoading(element) {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    this.loadWidget(entry.target);
                    observer.unobserve(entry.target);
                }
            });
        });

        observer.observe(element);
    }

    setupGesture(element, gesture) {
        switch (gesture) {
            case 'tap':
                element.addEventListener('click', (e) => {
                    this.handleWidgetTap(element, e);
                });
                break;
            case 'double_tap':
                let tapCount = 0;
                element.addEventListener('click', (e) => {
                    tapCount++;
                    setTimeout(() => {
                        if (tapCount === 2) {
                            this.handleWidgetDoubleTap(element, e);
                        }
                        tapCount = 0;
                    }, 300);
                });
                break;
            case 'long_press':
                let pressTimer;
                element.addEventListener('touchstart', (e) => {
                    pressTimer = setTimeout(() => {
                        this.handleWidgetLongPress(element, e);
                    }, 500);
                });
                element.addEventListener('touchend', () => {
                    clearTimeout(pressTimer);
                });
                break;
        }
    }

    loadWidget(element) {
        element.classList.add('loading');
        // Simulate widget loading
        setTimeout(() => {
            element.classList.remove('loading');
            element.classList.add('loaded');
        }, 500);
    }

    handleWidgetTap(element, event) {
        // Add haptic feedback
        if ('vibrate' in navigator) {
            navigator.vibrate(10);
        }

        // Widget-specific tap handling
        console.log('Widget tapped:', element.id);
    }

    handleWidgetDoubleTap(element, event) {
        // Fullscreen or expand widget
        element.classList.toggle('fullscreen');
    }

    handleWidgetLongPress(element, event) {
        // Show context menu
        this.showContextMenu(element, event);
    }

    showContextMenu(element, event) {
        // Implementation for context menu
        console.log('Context menu for:', element.id);
    }

    refreshData() {
        // Pull-to-refresh implementation
        document.body.classList.add('refreshing');
        setTimeout(() => {
            document.body.classList.remove('refreshing');
            this.loadFreshData();
        }, 1000);
    }

    loadFreshData() {
        // Load fresh data from API
        console.log('Loading fresh data...');
    }

    toggleFilters() {
        const filterPanel = document.querySelector('.mobile-filter-panel');
        if (filterPanel) {
            filterPanel.classList.toggle('visible');
        }
    }

    navigateNext() {
        // Navigate to next dashboard page
        console.log('Navigate next');
    }

    navigatePrevious() {
        // Navigate to previous dashboard page
        console.log('Navigate previous');
    }

    reconfigureLayout() {
        // Reconfigure layout for new breakpoint
        console.log('Reconfiguring layout for:', this.currentBreakpoint);
        location.reload(); // Simple solution - in production, would update layout dynamically
    }
}

// Initialize mobile dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const config = window.dashboardConfig || {};
    window.mobileDashboard = new MobileDashboard(config);
});
"""

        return js_code

    def get_mobile_dashboard_html_template(self, layout_config: Dict[str, Any]) -> str:
        """Generate complete HTML template for mobile dashboard."""

        widgets_html = []
        for widget in layout_config["widgets"]:
            widget_html = self._generate_widget_html(widget)
            widgets_html.append(widget_html)

        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes">
    <meta name="theme-color" content="#0066cc">
    <title>Mobile Analytics Dashboard</title>

    <!-- PWA manifest -->
    <link rel="manifest" href="/manifest.json">

    <!-- Responsive CSS -->
    <style>
        {self.generate_mobile_css(layout_config)}
    </style>

    <!-- Preload critical resources -->
    <link rel="preload" href="/fonts/dashboard-font.woff2" as="font" type="font/woff2" crossorigin>

    <!-- iOS specific meta tags -->
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-status-bar-style" content="default">
    <meta name="apple-mobile-web-app-title" content="Analytics Dashboard">
    <link rel="apple-touch-icon" href="/icons/apple-touch-icon.png">
</head>
<body class="mobile-dashboard">
    <!-- Loading screen -->
    <div id="loading-screen" class="loading-screen">
        <div class="loading-spinner"></div>
        <p>Loading Dashboard...</p>
    </div>

    <!-- Main dashboard container -->
    <main class="dashboard-container">
        <!-- Header with navigation -->
        <header class="dashboard-header">
            <h1 class="dashboard-title">Analytics</h1>
            <button class="menu-toggle touch-target" aria-label="Toggle menu">
                <span class="hamburger"></span>
            </button>
        </header>

        <!-- Dashboard grid -->
        <div class="dashboard-grid">
            {"".join(widgets_html)}
        </div>

        <!-- Bottom navigation for mobile -->
        <nav class="bottom-navigation" aria-label="Main navigation">
            <button class="nav-item touch-target active" data-page="overview">
                <span class="nav-icon">📊</span>
                <span class="nav-label">Overview</span>
            </button>
            <button class="nav-item touch-target" data-page="analytics">
                <span class="nav-icon">📈</span>
                <span class="nav-label">Analytics</span>
            </button>
            <button class="nav-item touch-target" data-page="reports">
                <span class="nav-icon">📋</span>
                <span class="nav-label">Reports</span>
            </button>
            <button class="nav-item touch-target" data-page="settings">
                <span class="nav-icon">⚙️</span>
                <span class="nav-label">Settings</span>
            </button>
        </nav>
    </main>

    <!-- Offline indicator -->
    <div id="offline-indicator" class="offline-indicator hidden">
        <span>You're offline. Some features may not be available.</span>
    </div>

    <!-- Configuration script -->
    <script>
        window.dashboardConfig = {JSON.dumps(layout_config, default=str)};
    </script>

    <!-- Dashboard JavaScript -->
    <script>
        {self.generate_mobile_javascript(layout_config)}
    </script>

    <!-- Service Worker registration -->
    <script>
        if ('serviceWorker' in navigator) {{
            window.addEventListener('load', () => {{
                navigator.serviceWorker.register('/sw.js');
            }});
        }}
    </script>
</body>
</html>
"""

        return html_template

    def _generate_widget_html(self, widget: Dict[str, Any]) -> str:
        """Generate HTML for individual widget."""
        widget_id = widget["widget_id"]
        widget_type = widget["widget_type"]
        title = widget["title"]
        config = widget["config"]

        base_classes = f"{widget_id} mobile-widget"

        if widget_type == "kpi_card":
            return f"""
            <div id="{widget_id}" class="{base_classes} mobile-kpi-card touch-target">
                <div class="kpi-header">
                    <h3 class="kpi-title">{title}</h3>
                </div>
                <div class="kpi-content">
                    <div class="kpi-value">$2.8M</div>
                    <div class="kpi-change positive">+15.3%</div>
                </div>
                <div class="kpi-sparkline"></div>
            </div>
            """

        elif widget_type == "chart":
            return f"""
            <div id="{widget_id}" class="{base_classes} mobile-chart pinch-zoomable">
                <div class="chart-header">
                    <h3 class="chart-title">{title}</h3>
                    <button class="chart-fullscreen touch-target" aria-label="Fullscreen">⛶</button>
                </div>
                <div class="chart-container">
                    <canvas class="chart-canvas" aria-label="Data visualization chart"></canvas>
                </div>
                <div class="chart-controls">
                    <button class="chart-prev touch-target" aria-label="Previous">‹</button>
                    <span class="chart-period">Last 30 days</span>
                    <button class="chart-next touch-target" aria-label="Next">›</button>
                </div>
            </div>
            """

        elif widget_type == "data_table":
            layout = config.get("layout", "full_table")

            if layout == "card_list":
                return f"""
                <div id="{widget_id}" class="{base_classes} mobile-table-cards">
                    <div class="table-header">
                        <h3 class="table-title">{title}</h3>
                    </div>
                    <div class="card-list scrollable">
                        <div class="data-card touch-target">
                            <div class="card-name">Mobile Users</div>
                            <div class="card-value">45,231</div>
                            <div class="card-change positive">+8.2%</div>
                        </div>
                        <div class="data-card touch-target">
                            <div class="card-name">Tablet Users</div>
                            <div class="card-value">12,847</div>
                            <div class="card-change negative">-2.1%</div>
                        </div>
                    </div>
                </div>
                """
            else:
                return f"""
                <div id="{widget_id}" class="{base_classes} mobile-table">
                    <div class="table-header">
                        <h3 class="table-title">{title}</h3>
                    </div>
                    <div class="table-container scrollable">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>Device</th>
                                    <th>Users</th>
                                    <th>Change</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr class="touch-target">
                                    <td>Mobile</td>
                                    <td>45,231</td>
                                    <td class="positive">+8.2%</td>
                                </tr>
                                <tr class="touch-target">
                                    <td>Tablet</td>
                                    <td>12,847</td>
                                    <td class="negative">-2.1%</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                """

        elif widget_type == "filter_panel":
            return f"""
            <div id="{widget_id}" class="{base_classes} mobile-filter-panel">
                <div class="filter-header">
                    <h3 class="filter-title">{title}</h3>
                    <button class="filter-toggle touch-target" aria-label="Toggle filters">▼</button>
                </div>
                <div class="filter-content">
                    <div class="filter-group">
                        <label class="filter-label">Device Type</label>
                        <select class="filter-select touch-target">
                            <option>All Devices</option>
                            <option>Smartphone</option>
                            <option>Tablet</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label class="filter-label">Platform</label>
                        <div class="filter-buttons">
                            <button class="filter-button touch-target active">All</button>
                            <button class="filter-button touch-target">iOS</button>
                            <button class="filter-button touch-target">Android</button>
                        </div>
                    </div>
                </div>
            </div>
            """

        else:
            return f"""
            <div id="{widget_id}" class="{base_classes}">
                <div class="widget-header">
                    <h3 class="widget-title">{title}</h3>
                </div>
                <div class="widget-content">
                    <p>Widget content for {widget_type}</p>
                </div>
            </div>
            """