#!/bin/bash

sed -i.bak \
    -e '/^Hourly/d' \
    -e 's/^2050 Profiles/timesteps/g' \
    -e 's%^54789%01/01/2050 01:00%g' \
    ${@}
