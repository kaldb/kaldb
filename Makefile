SELENIUM_BASE_URL ?= http://127.0.0.1:8083/admin/
SELENIUM_HEADLESS ?= true
SELENIUM_RUNNER ?= uv run --with selenium
SELENIUM_SCREENSHOT_DIR ?= tmp/admin-ui-selenium
SELENIUM_TESTS ?= $(shell find tools -type f -name '*selenium*_test.py' | sort)
SELENIUM_TIMEOUT ?= 20

SELENIUM_HEADLESS_FLAG := $(if $(filter false False FALSE 0 no No NO off Off OFF,$(SELENIUM_HEADLESS)),--no-headless,--headless)

.PHONY: help selenium-tests selenium-tests-visible test-selenium

help:
	@printf '%s\n' 'Available targets:'
	@printf '  %-24s %s\n' 'selenium-tests' 'Run all Selenium smoke tests headlessly.'
	@printf '  %-24s %s\n' 'selenium-tests-visible' 'Run all Selenium smoke tests with a visible browser.'
	@printf '  %-24s %s\n' 'test-selenium' 'Alias for selenium-tests.'
	@printf '%s\n' ''
	@printf '%s\n' 'Useful overrides:'
	@printf '  %-24s %s\n' 'SELENIUM_BASE_URL' 'Admin UI URL, defaults to http://127.0.0.1:8083/admin/.'
	@printf '  %-24s %s\n' 'SELENIUM_HEADLESS=false' 'Show Chrome while the tests run.'
	@printf '  %-24s %s\n' 'SELENIUM_TIMEOUT=30' 'Increase Selenium explicit wait timeout.'
	@printf '  %-24s %s\n' 'SELENIUM_TESTS="path ..."' 'Run a specific Selenium test script list.'

selenium-tests:
	@if [ -z "$(SELENIUM_TESTS)" ]; then \
		echo "No Selenium tests found."; \
		exit 1; \
	fi
	@set -e; \
	for test_script in $(SELENIUM_TESTS); do \
		echo "[selenium] $$test_script"; \
		$(SELENIUM_RUNNER) "$$test_script" \
			--base-url "$(SELENIUM_BASE_URL)" \
			$(SELENIUM_HEADLESS_FLAG) \
			--timeout "$(SELENIUM_TIMEOUT)" \
			--screenshot-dir "$(SELENIUM_SCREENSHOT_DIR)"; \
	done

selenium-tests-visible:
	@$(MAKE) selenium-tests SELENIUM_HEADLESS=false

test-selenium: selenium-tests
