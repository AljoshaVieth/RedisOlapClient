#!lua name=olaplib

local function flattenTable(tbl)
    local flattened = {}

    for _, value in ipairs(tbl) do
        if type(value) == "table" then
            local nested = flattenTable(value)
            for _, nestedValue in ipairs(nested) do
                table.insert(flattened, nestedValue)
            end
        else
            table.insert(flattened, value)
        end
    end

    return flattened
end


local function querySpecificDocuments(keys, args)
    return redis.call("FT.SEARCH", "date-index", "@d_year:[1993 1993]")
end

-- args[1] = index-name
-- args[2] = query
-- args[3] = fields to return
local function queryDocuments(keys, args)
    return redis.call("FT.SEARCH", args[1], args[2])
end

local function splitStringIntoArray(string, pattern)
    local words = {}
    for word in string:gmatch(pattern) do
        table.insert(words, word)
    end
    return words
end


-- This function is used to extract values of one specific field of queryDocuments
-- args[1] = index-name
-- args[2] = query
-- args[3] = the field to return
-- args[4] = the field to search for
local function queryFilterCriteria(keys, args)
    local query_result = redis.call("FT.SEARCH", args[1], args[2], "RETURN", 1, args[3], "LIMIT", 0, 2147483647) -- setting the limit to highest 32-bit int
    local result = ""
    local flattened = flattenTable(query_result)
    local uniqueDocuments = 0
    for i = 1, #flattened do
        if flattened[i] == args[3] then
            uniqueDocuments = uniqueDocuments + 1
            --table.insert(result, flattened[i+1])
            result = result .. "@" .. args[4] .. ":[" .. flattened[i + 1] .. " " .. flattened[i + 1] .. "] | "
            -- build search string for lineorder...
        end
    end
    result = string.sub(result, 1, -3)
    return result
end


local function runQ1_1(keys, args)
    local queryFilter = queryFilterCriteria(keys, args)
    local query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[1 3] @lo_quantity:[0 24]" ..queryFilter, "LIMIT", 0, 2147483647)
    local flattened = flattenTable(query_result)
    local currentLoExtendedPrice = 0
    local currentLoDiscount = 0
    local revenue = 0

    for i = 1, #flattened do
        if flattened[i] == "lo_extendedprice" then
            currentLoExtendedPrice = flattened[i + 1]
        elseif flattened[i] == "lo_discount" then
            currentLoDiscount = flattened[i+1]
            revenue = revenue + currentLoDiscount * currentLoExtendedPrice
        end
    end
    return "revenue: "..revenue
end

redis.register_function('querySpecificDocuments', querySpecificDocuments)
redis.register_function('queryDocuments', queryDocuments)
redis.register_function('queryFilterCriteria', queryFilterCriteria)
redis.register_function('runQ1_1', runQ1_1)





