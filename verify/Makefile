# Proof driver. Run from the repository root:
#
#   make -f verify/Makefile verify | verify-varint | build | docker-verify
#
# Modules own their -wp-fct lists and flags in their CMakeLists.txt; this only
# configures build directories and fans out.

# pipefail: the targets pipe frama-c through tee, whose status is always 0.
SHELL      := /bin/bash
.SHELLFLAGS := -o pipefail -c

ROOT       := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
BUILD      := $(ROOT)/build-verify
LOGS       := $(BUILD)/logs
CHECK      := $(ROOT)/verify/check-proved.sh
SELF       := $(lastword $(MAKEFILE_LIST))
IMAGE      ?= normfs-verify:pinned

# Discovered, not listed, so it cannot go stale.
MODULES    := $(patsubst $(ROOT)/%/CMakeLists.txt,%,$(wildcard $(ROOT)/*/c/CMakeLists.txt))

# One line per proof target, naming its build directory.
TARGETS    := verify-varint
dir-verify-varint := uintn-rs_c

CMAKE_ARGS := -DCMAKE_BUILD_TYPE=Release

.PHONY: all verify $(TARGETS) configure build clean docker-image docker-verify

all: verify

configure:
	@test -n "$(MODULES)" || { echo "no c/CMakeLists.txt found under $(ROOT)" >&2; exit 1; }
	@for m in $(MODULES); do \
	    echo "== configure $$m =="; \
	    cmake -S "$(ROOT)/$$m" -B "$(BUILD)/$$(echo $$m | tr / _)" \
	          $(CMAKE_ARGS) >/dev/null || exit 1; \
	done

build: configure
	@for m in $(MODULES); do \
	    echo "== build $$m =="; \
	    cmake --build "$(BUILD)/$$(echo $$m | tr / _)" || exit 1; \
	done

# frama-c exits 0 even with goals left open, so check-proved.sh rules instead.
$(TARGETS): verify-%: configure
	@mkdir -p "$(LOGS)"
	cmake --build "$(BUILD)/$(dir-verify-$*)" --target verify-$* \
	    2>&1 | tee "$(LOGS)/verify-$*.log"
	@"$(CHECK)" "$(LOGS)/verify-$*.log"

# Runs all targets before failing, so one broken target does not hide the rest.
verify: configure
	@rc=0; \
	for t in $(TARGETS); do \
	    echo "== $$t =="; \
	    $(MAKE) -f "$(SELF)" $$t || rc=1; \
	done; \
	exit $$rc

docker-image:
	docker build -t $(IMAGE) -f $(ROOT)/verify/Dockerfile $(ROOT)/verify

docker-verify: docker-image
	docker run --rm -v $(ROOT):/work -w /work $(IMAGE) \
	    make -f verify/Makefile verify

clean:
	rm -rf "$(BUILD)"
