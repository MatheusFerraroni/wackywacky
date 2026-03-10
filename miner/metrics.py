from opentelemetry import metrics

meter = metrics.get_meter('miner')

metric_requests_started = meter.create_counter(
    'requests_started',
    description='Total executions of requester.request',
)

metric_requests_made = meter.create_counter(
    'requests_made',
    description='Total requests actually made',
)

metric_requests_failed_max_retry = meter.create_counter(
    'requests_failed_max_retry',
    description='Total number of failed ignored pages due to max_retry reached',
)

metric_requests_reached_recursion_limit = meter.create_counter(
    'requests_reached_recursion_limit',
    description='Total number of failed requests due to reached max_recursion',
)

metric_requests_domain_blocked = meter.create_counter(
    'requests_domain_blocked',
    description='Total number of domains blocked',
)

metric_requests_failed = meter.create_counter(
    'requests_failed',
    description='Total number of failed requests page.goto',
)

metric_requests_failed_status_code = meter.create_counter(
    'requests_failed_status_code',
    description='Total number of failed requests due to not returning or 400',
)

metric_request_domain_in_cooldown = meter.create_counter(
    'request_domain_in_cooldown',
    description='Total number of ignored page due to domain in cooldown',
)

metric_pages_saved = meter.create_counter(
    'pages_saved',
    description='Total number of URLs requested and persisted',
)

metric_pages_saved_with_status = meter.create_counter(
    'pages_saved_with_status',
    description='Total number of URLs requested and persisted with status',
)

metric_pages_released = meter.create_counter(
    'pages_released',
    description='Total number of pages released in clean_db',
)

metric_pages_marked_as_same_as = meter.create_counter(
    'pages_marked_as_same_as',
    description='Total pages marked as same_as',
)

metric_threads_alive = meter.create_up_down_counter(
    'threads_alive',
    description='Number of worker threads currently alive',
)

metric_request_duration = meter.create_histogram(
    'request_duration',
    unit='s',
    description='Total duration of completed requester.request',
)

metric_any_request_duration = meter.create_histogram(
    'any_request_duration',
    unit='s',
    description='Total duration of any requester.request',
)

metric_page_goto_duration = meter.create_histogram(
    'page_goto_duration',
    unit='s',
    description='Duration of page.goto',
)

metric_clean_db_duration = meter.create_histogram(
    'clean_db_duration',
    unit='s',
    description='Duration of app.clean_db',
)

metric_domain_check_duration = meter.create_histogram(
    'domain_check_duration',
    unit='s',
    description='Duration of domain check',
)

metric_saving_found_hrefs_duration = meter.create_histogram(
    'saving_found_hrefs_duration',
    unit='s',
    description='Duration of saving hrefs',
)
