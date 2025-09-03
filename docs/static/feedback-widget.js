/**
 * Embeddable Documentation Feedback Widget
 * Lightweight widget that can be included in any documentation page
 */

(function() {
    'use strict';
    
    // Configuration
    const FEEDBACK_CONFIG = {
        apiEndpoint: '/api/v1/feedback',
        position: 'bottom-right', // bottom-right, bottom-left, top-right, top-left
        theme: 'light', // light, dark, auto
        collectAnalytics: true,
        autoShow: false, // Show widget automatically after time delay
        autoShowDelay: 30000, // 30 seconds
        localStorageKey: 'documentation_feedback',
        version: '1.0.0'
    };
    
    class FeedbackWidget {
        constructor(config = {}) {
            this.config = { ...FEEDBACK_CONFIG, ...config };
            this.isVisible = false;
            this.rating = 0;
            this.selectedTags = new Set();
            this.pageInfo = this.collectPageInfo();
            
            this.init();
        }
        
        init() {
            this.injectStyles();
            this.createWidget();
            this.bindEvents();
            
            if (this.config.autoShow) {
                setTimeout(() => {
                    this.show();
                }, this.config.autoShowDelay);
            }
            
            // Sync any stored feedback when online
            if (navigator.onLine) {
                this.syncStoredFeedback();
            }
        }
        
        injectStyles() {
            if (document.getElementById('feedback-widget-styles')) return;
            
            const styles = `
                .feedback-widget {
                    position: fixed;
                    z-index: 999999;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    font-size: 14px;
                    line-height: 1.5;
                }
                
                .feedback-widget.bottom-right { bottom: 20px; right: 20px; }
                .feedback-widget.bottom-left { bottom: 20px; left: 20px; }
                .feedback-widget.top-right { top: 20px; right: 20px; }
                .feedback-widget.top-left { top: 20px; left: 20px; }
                
                .feedback-trigger {
                    background: #4f46e5;
                    color: white;
                    border: none;
                    border-radius: 50px;
                    padding: 12px 20px;
                    cursor: pointer;
                    box-shadow: 0 4px 12px rgba(79, 70, 229, 0.3);
                    transition: all 0.3s ease;
                    font-size: 14px;
                    font-weight: 500;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }
                
                .feedback-trigger:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 6px 16px rgba(79, 70, 229, 0.4);
                }
                
                .feedback-panel {
                    position: absolute;
                    background: white;
                    border-radius: 12px;
                    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.15);
                    width: 320px;
                    transform: scale(0.95) translateY(10px);
                    opacity: 0;
                    visibility: hidden;
                    transition: all 0.3s ease;
                    border: 1px solid #e5e7eb;
                    bottom: 60px;
                    right: 0;
                }
                
                .feedback-widget.top-right .feedback-panel,
                .feedback-widget.top-left .feedback-panel {
                    bottom: auto;
                    top: 60px;
                }
                
                .feedback-widget.bottom-left .feedback-panel,
                .feedback-widget.top-left .feedback-panel {
                    right: auto;
                    left: 0;
                }
                
                .feedback-panel.visible {
                    transform: scale(1) translateY(0);
                    opacity: 1;
                    visibility: visible;
                }
                
                .feedback-header {
                    padding: 20px 20px 15px;
                    border-bottom: 1px solid #e5e7eb;
                }
                
                .feedback-title {
                    margin: 0 0 5px;
                    font-size: 16px;
                    font-weight: 600;
                    color: #1f2937;
                }
                
                .feedback-subtitle {
                    margin: 0;
                    color: #6b7280;
                    font-size: 13px;
                }
                
                .feedback-content {
                    padding: 20px;
                }
                
                .feedback-question {
                    margin-bottom: 15px;
                    font-weight: 500;
                    color: #374151;
                }
                
                .rating-stars {
                    display: flex;
                    justify-content: center;
                    gap: 8px;
                    margin-bottom: 20px;
                }
                
                .rating-star {
                    width: 28px;
                    height: 28px;
                    cursor: pointer;
                    color: #d1d5db;
                    transition: color 0.2s ease;
                }
                
                .rating-star:hover,
                .rating-star.active {
                    color: #fbbf24;
                }
                
                .feedback-tags {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 8px;
                    margin-bottom: 15px;
                }
                
                .feedback-tag {
                    padding: 6px 12px;
                    background: #f3f4f6;
                    border: 1px solid #e5e7eb;
                    border-radius: 20px;
                    font-size: 12px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                    color: #6b7280;
                }
                
                .feedback-tag:hover {
                    background: #e5e7eb;
                }
                
                .feedback-tag.selected {
                    background: #dbeafe;
                    border-color: #3b82f6;
                    color: #1d4ed8;
                }
                
                .feedback-textarea {
                    width: 100%;
                    min-height: 60px;
                    padding: 10px;
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    resize: vertical;
                    font-size: 13px;
                    font-family: inherit;
                    margin-bottom: 15px;
                    box-sizing: border-box;
                }
                
                .feedback-textarea:focus {
                    outline: none;
                    border-color: #3b82f6;
                    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
                }
                
                .feedback-actions {
                    display: flex;
                    gap: 8px;
                }
                
                .feedback-btn {
                    flex: 1;
                    padding: 10px 16px;
                    border: none;
                    border-radius: 6px;
                    cursor: pointer;
                    font-size: 13px;
                    font-weight: 500;
                    transition: all 0.2s ease;
                }
                
                .feedback-btn-primary {
                    background: #4f46e5;
                    color: white;
                }
                
                .feedback-btn-primary:hover {
                    background: #4338ca;
                }
                
                .feedback-btn-secondary {
                    background: #f9fafb;
                    color: #6b7280;
                    border: 1px solid #e5e7eb;
                }
                
                .feedback-btn-secondary:hover {
                    background: #f3f4f6;
                }
                
                .feedback-success {
                    text-align: center;
                    padding: 30px 20px;
                    color: #059669;
                }
                
                .feedback-success-icon {
                    width: 40px;
                    height: 40px;
                    margin: 0 auto 15px;
                    color: #10b981;
                }
                
                .feedback-icon {
                    width: 16px;
                    height: 16px;
                    fill: currentColor;
                }
                
                @media (max-width: 480px) {
                    .feedback-panel {
                        width: calc(100vw - 40px);
                        left: 20px !important;
                        right: 20px !important;
                    }
                }
                
                .feedback-widget.theme-dark {
                    color: #f9fafb;
                }
                
                .feedback-widget.theme-dark .feedback-panel {
                    background: #1f2937;
                    border-color: #374151;
                    color: #f9fafb;
                }
                
                .feedback-widget.theme-dark .feedback-title {
                    color: #f9fafb;
                }
                
                .feedback-widget.theme-dark .feedback-subtitle {
                    color: #9ca3af;
                }
                
                .feedback-widget.theme-dark .feedback-textarea {
                    background: #374151;
                    border-color: #4b5563;
                    color: #f9fafb;
                }
                
                .feedback-widget.theme-dark .feedback-tag {
                    background: #374151;
                    border-color: #4b5563;
                    color: #d1d5db;
                }
            `;
            
            const styleSheet = document.createElement('style');
            styleSheet.id = 'feedback-widget-styles';
            styleSheet.textContent = styles;
            document.head.appendChild(styleSheet);
        }
        
        createWidget() {
            const widget = document.createElement('div');
            widget.className = `feedback-widget ${this.config.position} theme-${this.config.theme}`;
            widget.id = 'documentation-feedback-widget';
            
            widget.innerHTML = `
                <button class="feedback-trigger" id="feedback-trigger">
                    <svg class="feedback-icon" viewBox="0 0 24 24">
                        <path d="M20 2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h4l4 4 4-4h4c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-6 11H6v-2h8v2zm0-3H6V8h8v2zm2-3H6V5h10v2z"/>
                    </svg>
                    Feedback
                </button>
                
                <div class="feedback-panel" id="feedback-panel">
                    <div class="feedback-header">
                        <h3 class="feedback-title">How was this page?</h3>
                        <p class="feedback-subtitle">Help us improve our documentation</p>
                    </div>
                    
                    <div class="feedback-content">
                        <div id="feedback-form">
                            <div class="feedback-question">Rate this page:</div>
                            <div class="rating-stars" id="rating-stars">
                                ${[1,2,3,4,5].map(i => `
                                    <svg class="rating-star" data-rating="${i}" viewBox="0 0 24 24">
                                        <path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/>
                                    </svg>
                                `).join('')}
                            </div>
                            
                            <div class="feedback-tags" id="feedback-tags">
                                <span class="feedback-tag" data-tag="helpful">Helpful</span>
                                <span class="feedback-tag" data-tag="clear">Clear</span>
                                <span class="feedback-tag" data-tag="accurate">Accurate</span>
                                <span class="feedback-tag" data-tag="outdated">Outdated</span>
                                <span class="feedback-tag" data-tag="confusing">Confusing</span>
                                <span class="feedback-tag" data-tag="incomplete">Incomplete</span>
                            </div>
                            
                            <textarea 
                                class="feedback-textarea" 
                                id="feedback-comment" 
                                placeholder="Any additional comments? (optional)"
                            ></textarea>
                            
                            <div class="feedback-actions">
                                <button class="feedback-btn feedback-btn-secondary" id="feedback-cancel">
                                    Skip
                                </button>
                                <button class="feedback-btn feedback-btn-primary" id="feedback-submit">
                                    Send Feedback
                                </button>
                            </div>
                        </div>
                        
                        <div id="feedback-success" style="display: none;">
                            <div class="feedback-success">
                                <svg class="feedback-success-icon" viewBox="0 0 24 24">
                                    <path fill="currentColor" d="M12 2C6.5 2 2 6.5 2 12S6.5 22 12 22 22 17.5 22 12 17.5 2 12 2M10 17L5 12L6.41 10.59L10 14.17L17.59 6.58L19 8L10 17Z"/>
                                </svg>
                                <h3>Thanks for your feedback!</h3>
                                <p>Your input helps us improve.</p>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            document.body.appendChild(widget);
        }
        
        bindEvents() {
            const trigger = document.getElementById('feedback-trigger');
            const panel = document.getElementById('feedback-panel');
            const cancelBtn = document.getElementById('feedback-cancel');
            const submitBtn = document.getElementById('feedback-submit');
            
            // Toggle panel
            trigger.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggle();
            });
            
            // Close when clicking outside
            document.addEventListener('click', (e) => {
                if (!document.getElementById('documentation-feedback-widget').contains(e.target)) {
                    this.hide();
                }
            });
            
            // Rating stars
            document.querySelectorAll('.rating-star').forEach(star => {
                star.addEventListener('click', () => {
                    this.setRating(parseInt(star.dataset.rating));
                });
                
                star.addEventListener('mouseenter', () => {
                    this.highlightStars(parseInt(star.dataset.rating));
                });
            });
            
            document.getElementById('rating-stars').addEventListener('mouseleave', () => {
                this.highlightStars(this.rating);
            });
            
            // Tags
            document.querySelectorAll('.feedback-tag').forEach(tag => {
                tag.addEventListener('click', () => {
                    this.toggleTag(tag.dataset.tag, tag);
                });
            });
            
            // Buttons
            cancelBtn.addEventListener('click', () => this.hide());
            submitBtn.addEventListener('click', () => this.submitFeedback());
            
            // Prevent panel clicks from closing
            panel.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        }
        
        collectPageInfo() {
            return {
                url: window.location.href,
                pathname: window.location.pathname,
                title: document.title,
                userAgent: navigator.userAgent,
                referrer: document.referrer,
                timestamp: new Date().toISOString(),
                viewport: {
                    width: window.innerWidth,
                    height: window.innerHeight
                },
                language: navigator.language || navigator.userLanguage
            };
        }
        
        show() {
            const panel = document.getElementById('feedback-panel');
            panel.classList.add('visible');
            this.isVisible = true;
            
            // Track show event
            this.trackEvent('feedback_shown');
        }
        
        hide() {
            const panel = document.getElementById('feedback-panel');
            panel.classList.remove('visible');
            this.isVisible = false;
        }
        
        toggle() {
            if (this.isVisible) {
                this.hide();
            } else {
                this.show();
            }
        }
        
        setRating(rating) {
            this.rating = rating;
            this.highlightStars(rating);
        }
        
        highlightStars(count) {
            document.querySelectorAll('.rating-star').forEach((star, index) => {
                if (index < count) {
                    star.classList.add('active');
                } else {
                    star.classList.remove('active');
                }
            });
        }
        
        toggleTag(tag, element) {
            if (this.selectedTags.has(tag)) {
                this.selectedTags.delete(tag);
                element.classList.remove('selected');
            } else {
                this.selectedTags.add(tag);
                element.classList.add('selected');
            }
        }
        
        async submitFeedback() {
            const comment = document.getElementById('feedback-comment').value;
            
            const feedbackData = {
                type: 'page_feedback',
                rating: this.rating,
                tags: Array.from(this.selectedTags),
                comment: comment.trim(),
                page: this.pageInfo,
                widget_version: this.config.version
            };
            
            try {
                const response = await fetch(this.config.apiEndpoint, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(feedbackData)
                });
                
                if (response.ok) {
                    this.showSuccess();
                    this.trackEvent('feedback_submitted', {
                        rating: this.rating,
                        has_comment: comment.length > 0,
                        tags_count: this.selectedTags.size
                    });
                } else {
                    throw new Error('Submit failed');
                }
            } catch (error) {
                console.warn('Feedback submission failed, storing locally:', error);
                this.storeFeedbackLocally(feedbackData);
                this.showSuccess(); // Still show success to user
            }
        }
        
        showSuccess() {
            document.getElementById('feedback-form').style.display = 'none';
            document.getElementById('feedback-success').style.display = 'block';
            
            // Auto-hide after 3 seconds
            setTimeout(() => {
                this.hide();
                this.reset();
            }, 3000);
        }
        
        reset() {
            // Reset form state
            this.rating = 0;
            this.selectedTags.clear();
            
            // Reset UI
            document.getElementById('feedback-comment').value = '';
            document.querySelectorAll('.rating-star').forEach(star => {
                star.classList.remove('active');
            });
            document.querySelectorAll('.feedback-tag').forEach(tag => {
                tag.classList.remove('selected');
            });
            
            // Show form, hide success
            document.getElementById('feedback-form').style.display = 'block';
            document.getElementById('feedback-success').style.display = 'none';
        }
        
        storeFeedbackLocally(feedbackData) {
            const stored = JSON.parse(localStorage.getItem(this.config.localStorageKey) || '[]');
            stored.push({
                ...feedbackData,
                stored_at: new Date().toISOString(),
                synced: false
            });
            localStorage.setItem(this.config.localStorageKey, JSON.stringify(stored));
        }
        
        async syncStoredFeedback() {
            const stored = JSON.parse(localStorage.getItem(this.config.localStorageKey) || '[]');
            const unsynced = stored.filter(item => !item.synced);
            
            if (unsynced.length === 0) return;
            
            for (const feedback of unsynced) {
                try {
                    const response = await fetch(this.config.apiEndpoint, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            ...feedback,
                            _stored_sync: true
                        })
                    });
                    
                    if (response.ok) {
                        feedback.synced = true;
                    }
                } catch (error) {
                    console.warn('Failed to sync stored feedback:', error);
                    break; // Stop trying if network is unavailable
                }
            }
            
            // Update stored data
            localStorage.setItem(this.config.localStorageKey, JSON.stringify(stored));
        }
        
        trackEvent(event, data = {}) {
            if (!this.config.collectAnalytics) return;
            
            // Google Analytics 4
            if (typeof gtag !== 'undefined') {
                gtag('event', event, {
                    ...data,
                    page_path: this.pageInfo.pathname,
                    page_title: this.pageInfo.title
                });
            }
            
            // Custom analytics endpoint
            if (this.config.analyticsEndpoint) {
                fetch(this.config.analyticsEndpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        event,
                        data,
                        page: this.pageInfo,
                        timestamp: new Date().toISOString()
                    })
                }).catch(() => {}); // Fail silently
            }
        }
        
        destroy() {
            const widget = document.getElementById('documentation-feedback-widget');
            if (widget) {
                widget.remove();
            }
            
            const styles = document.getElementById('feedback-widget-styles');
            if (styles) {
                styles.remove();
            }
        }
    }
    
    // Auto-initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            window.DocumentationFeedback = new FeedbackWidget(window.FEEDBACK_CONFIG || {});
        });
    } else {
        window.DocumentationFeedback = new FeedbackWidget(window.FEEDBACK_CONFIG || {});
    }
    
    // Expose class for manual initialization
    window.FeedbackWidget = FeedbackWidget;
    
})();