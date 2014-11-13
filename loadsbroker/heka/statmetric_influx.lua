-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "cjson"
require "string"
require "table"

local prefix = read_config("prefix")
local suffix = read_config("suffix")

local function annotate(name)
    if not prefix and not suffix then return name end
    local parts = {}
    for part in string.gmatch(name, "[^.]+") do
        parts[#parts+1] = part
    end
    if prefix then table.insert(parts, 3, prefix) end
    if suffix then table.insert(parts, #parts, suffix) end
    return table.concat(parts, ".")
end

function process_message()
    local output = {}
    local ts = tonumber(read_message("Fields[timestamp]"))
    if not ts then return -1 end
    ts = ts * 1000
    while true do
        typ, name, value, representation, count = read_next_field()
        if not typ then break end

        if name ~= "timestamp" and typ ~= 1 then -- exclude bytes
            local stat = {
                name = annotate(name),
                columns = {"time", "value"},
                points = {{ts, value}}
            }
            output[#output+1] = stat
        end
    end
    inject_payload("json", "influx_stats", cjson.encode(output))
    return 0
end
