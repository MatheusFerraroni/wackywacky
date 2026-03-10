from opentelemetry import metrics

meter = metrics.get_meter('miner')

metric_requests_total_started = meter.create_counter(
    'metric_requests_total_started',
    description='Total executions of requester.request',
)

metric_requests_total_made = meter.create_counter(
    'metric_requests_total_made',
    description='Total requests actually made',
)

metric_requests_failed_max_retry_total = meter.create_counter(
    'metric_requests_failed_max_retry_total',
    description='Total number of failed ignored pages due to max_retry reached',
)

metric_requests_reached_recursion_limit_total = meter.create_counter(
    'metric_requests_reached_recursion_limit_total',
    description='Total number of failed requests due to reached max_recursion',
)

metric_requests_domain_blocked_total = meter.create_counter(
    'metric_requests_domain_blocked_total',
    description='Total number of domains blocked',
)

metric_requests_failed_total = meter.create_counter(
    'metric_requests_failed_total',
    description='Total number of failed requests page.goto',
)

metric_requests_failed_status_code_total = meter.create_counter(
    'metric_requests_failed_status_code_total',
    description='Total number of failed requests due to not returning or 400',
)

metric_request_domain_in_cooldown = meter.create_counter(
    'metric_request_domain_in_cooldown',
    description='Total number of ignored page due to domain in cooldown',
)

metric_pages_saved_total = meter.create_counter(
    'metric_pages_saved_total',
    description='Total number of URLs requested and persisted',
)

metric_pages_saved_total_with_status = meter.create_counter(
    'metric_pages_saved_total',
    description='Total number of URLs requested and persisted with status',
)

metric_pages_released_total = meter.create_counter(
    'metric_pages_released_total',
    description='Total number of pages released in clean_db',
)

metric_request_duration_ms = meter.create_histogram(
    'metric_request_duration_ms',
    unit='ms',
    description='Total duration of completed requester.request',
)

metric_any_request_duration_ms = meter.create_histogram(
    'metric_any_request_duration_ms',
    unit='ms',
    description='Total duration of any requester.request',
)

metric_page_goto_duration_ms = meter.create_histogram(
    'metric_page_goto_duration_ms',
    unit='ms',
    description='Duration of page.goto',
)

metric_clean_db_duration_ms = meter.create_histogram(
    'metric_clean_db_duration_ms',
    unit='ms',
    description='Duration of app.clean_db',
)

metric_domain_check_duration_ms = meter.create_histogram(
    'metric_domain_check_duration_ms',
    unit='ms',
    description='Duration of domain check',
)
