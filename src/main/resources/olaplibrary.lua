#!lua name=olaplib

-- Helper methods:

-- This function takes a nested table as input and returns a flattened version of it
local function flattenTable(tbl)
    local flattened = {}  -- Create an empty table to store the flattened values
    local index = 1  -- Initialize the index for the flattened table
    local function flatten(value)
        if type(value) == "table" then
            -- If the value is a table, recursively flatten its elements
            for _, nestedValue in ipairs(value) do
                flatten(nestedValue)
            end
        else
            -- If the value is not a table, store it in the flattened table
            flattened[index] = value
            index = index + 1
        end
    end
    flatten(tbl)  -- Call the flatten function to start flattening the table
    return flattened  -- Return the flattened table
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

-- Splits a given string into an array of substrings based on a specified pattern.
local function splitStringIntoArray(string, pattern)
    local words = {}
    for word in string:gmatch(pattern) do
        table.insert(words, word)
    end
    return words
end


-- This function is used to perform a RediSearch query and build part of a new query-string based on the results
-- args[1] = index-name
-- args[2] = query
-- args[3] = the field to return
-- args[4] = the field to search for
-- keys is not used but required by how lua in redis works
local function queryFilterCriteria(keys, args, offset)
    redis.log(redis.LOG_WARNING, "offset " .. offset)
    redis.log(redis.LOG_WARNING, "index " .. args[offset + 1])
    local query_result = redis.call("FT.SEARCH", args[offset + 1], args[offset + 2], "RETURN", 1, args[offset + 3], "LIMIT", 0, 2147483647) -- setting the limit to highest 32-bit int
    local result = ""
    local flattened = flattenTable(query_result)
    for i = 1, #flattened do
        if flattened[i] == args[offset + 3] then
            result = result .. "@" .. args[offset + 4] .. ":[" .. flattened[i + 1] .. " " .. flattened[i + 1] .. "] | "
        end
    end
    result = string.sub(result, 1, -3)
    return result
end

-- Query methods

-- This executes Q1.1
-- Scala calls this function with args = ["date-index", "@d_year:[1993 1993]", "d_datekey", "lo_orderdate"]
-- keys is not used but required by how lua in redis works
local function runQ1_1_a(keys, args)
    -- Apply additional query filters based on provided arguments.
    local queryFilter = queryFilterCriteria(keys, args, 0)
    -- Search in 'lineorder-index' with specified discount & quantity ranges and the queryFilter
    local query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[1 3] @lo_quantity:[0 24]" .. queryFilter, "LIMIT", 0, 2147483647)
    local flattened = flattenTable(query_result)
    local currentLoExtendedPrice = 0
    local currentLoDiscount = 0
    local revenue = 0
    -- Calculate revenue by iterating over flattened results.
    for i = 1, #flattened do
        if flattened[i] == "lo_extendedprice" then
            currentLoExtendedPrice = flattened[i + 1]
        elseif flattened[i] == "lo_discount" then
            currentLoDiscount = flattened[i + 1]
            revenue = revenue + currentLoDiscount * currentLoExtendedPrice
        end
    end
    return "revenue: " .. revenue
end

-- This executes Q1.2
-- Scala calls this function with args = ["date-index", "@d_yearmonthnum:[199401 199401]", "d_datekey", "lo_orderdate"]
-- keys is not used but required by how lua in redis works
local function runQ1_2_a(keys, args)
    local queryFilter = queryFilterCriteria(keys, args, 0)
    local query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[4 6] @lo_quantity:[26 35]" .. queryFilter, "LIMIT", 0, 2147483647)
    local flattened = flattenTable(query_result)
    local currentLoExtendedPrice = 0
    local currentLoDiscount = 0
    local revenue = 0
    local totalDocuments = 0
    -- Calculate revenue by iterating over flattened results.
    for i = 1, #flattened do
        if flattened[i] == "lo_extendedprice" then
            currentLoExtendedPrice = flattened[i + 1]
        elseif flattened[i] == "lo_discount" then
            currentLoDiscount = flattened[i + 1]
            revenue = revenue + currentLoDiscount * currentLoExtendedPrice
            totalDocuments = totalDocuments + 1
        end
    end
    return "revenue: " .. revenue
end

-- This executes Q1.3
-- Scala calls this function with args = ["date-index", "@d_year:[1994 1994] @d_weeknuminyear:[6 6]", "d_datekey", "lo_orderdate"]
-- keys is not used but required by how lua in redis works
local function runQ1_3_a(keys, args)
    -- Apply additional query filters based on provided arguments.
    local queryFilter = queryFilterCriteria(keys, args, 0)
    -- Search in 'lineorder-index' with specified discount & quantity ranges and the queryFilter
    local query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[5 7] @lo_quantity:[26 35]" .. queryFilter, "LIMIT", 0, 2147483647)
    local flattened = flattenTable(query_result)
    local currentLoExtendedPrice = 0
    local currentLoDiscount = 0
    local revenue = 0
    -- Calculate revenue by iterating over flattened results.
    for i = 1, #flattened do
        if flattened[i] == "lo_extendedprice" then
            currentLoExtendedPrice = flattened[i + 1]
        elseif flattened[i] == "lo_discount" then
            currentLoDiscount = flattened[i + 1]
            revenue = revenue + currentLoDiscount * currentLoExtendedPrice
        end
    end
    return "revenue: " .. revenue
end

redis.register_function('querySpecificDocuments', querySpecificDocuments)
redis.register_function('queryDocuments', queryDocuments)
redis.register_function('queryFilterCriteria', queryFilterCriteria)
redis.register_function('runQ1_1_a', runQ1_1_a)
redis.register_function('runQ1_2_a', runQ1_2_a)
redis.register_function('runQ1_3_a', runQ1_3_a)


-- The following functions are different versions of the functions above or completely different approaches.
-- They all don´t work or are not fast enough compared to the methods above.

--[[
-- This function is an attempt to execute Q2.1 in lua.
-- It first queries three different indexes with dimension data and then builds a search string from these results.
-- As building the search string takes longer than the whole query in the client approach, this approach was abandoned prematurely.
-- Joining the data in the next step to perform the groupings would also be very difficult to implement in Lua.
local function runQ2_1_a(keys, args)
    -- Apply additional query filters based on provided arguments.
    local queryFilter1 = queryFilterCriteria(keys, args,0)
    local queryFilter2 = queryFilterCriteria(keys, args,4)
    local queryFilter3 = queryFilterCriteria(keys, args,8)
    local queryFilter = "("..queryFilter1 .. ") (" .. queryFilter2 .. ") (" .. queryFilter3..")"
    -- Search in 'lineorder-index' with specified ranges and the queryFilter
    local query_result = redis.call("FT.SEARCH", "lineorder-index", queryFilter, "RETURN", 4, "lo_revenue", "lo_orderdate", "lo_partkey", "lo_suppkey", "LIMIT", 0, 2147483647)
    local flattened = flattenTable(query_result)
    redis.log(redis.LOG_WARNING, "flattened " .. #flattened) -- Debug print the search string to redis console
    local revenue = 0 -- Return placeolder
    return "revenue: " .. revenue
end






local function runQ1_1_b(keys, args)
    local queriedDates = redis.call("FT.SEARCH", "date-index", "@d_year:[1993 1993]", "RETURN", 1, "d_datekey", "LIMIT", 0, 2147483647)
    table.remove(queriedDates, 1) -- removing the count of results from the table
    local sum_revenue = 0
    for k, v in pairs(queriedDates) do
        if not (v[2] == nil) then
            -- in lua 5.1 (which redis uses) there is no way to skip the element in the loop. In 5.2 this could be done with goto
            local lineorder_query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[1 3] @lo_quantity:[0 24]" .. "@lo_orderdate:[" .. v[2] .. " " .. v[2] .. "]", "RETURN", 2, "lo_extendedprice", "lo_discount", "LIMIT", 0, 2147483647)
            table.remove(lineorder_query_result, 1) -- removing the count of results from the table
            for x, y in pairs(lineorder_query_result) do
                if not (y[2] == nil) then
                    sum_revenue = sum_revenue + y[2] * y[4]
                end
            end
        end
    end
    return sum_revenue
end


local function runQ1_2_b(keys, args)
    local queriedDates = redis.call("FT.SEARCH", "date-index", "@d_yearmonthnum:[199401 199401]", "RETURN", 1, "d_datekey", "LIMIT", 0, 2147483647)
    table.remove(queriedDates, 1) -- removing the count of results from the table
    local sum_revenue = 0
    for k, v in pairs(queriedDates) do
        if not (v[2] == nil) then
            -- in lua 5.1 (which redis uses) there is no way to skip the element in the loop. In 5.2 this could be done with goto
            local lineorder_query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[4 6] @lo_quantity:[26 35]" .. "@lo_orderdate:[" .. v[2] .. " " .. v[2] .. "]", "RETURN", 2, "lo_extendedprice", "lo_discount", "LIMIT", 0, 2147483647)
            table.remove(lineorder_query_result, 1) -- removing the count of results from the table
            for x, y in pairs(lineorder_query_result) do
                if not (y[2] == nil) then
                    sum_revenue = sum_revenue + y[2] * y[4]
                end
            end
        end
    end
    return sum_revenue
end

local function runQ1_1_c(keys, args)
    local lo_orderdate_query_string = redis.call("GET", "yearDateIndex:1993")
    local lineorder_query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[1 3] @lo_quantity:[0 24]" ..lo_orderdate_query_string, "LIMIT", 0, 2147483647)
    local flattened = flattenTable(lineorder_query_result)
    local currentLoExtendedPrice = 0
    local currentLoDiscount = 0
    local revenue = 0
    for i = 1, #flattened do
        if flattened[i] == "lo_extendedprice" then
            currentLoExtendedPrice = flattened[i + 1]
        elseif flattened[i] == "lo_discount" then
            currentLoDiscount = flattened[i + 1]
            revenue = revenue + currentLoDiscount * currentLoExtendedPrice
        end
    end
    return "revenue: " .. revenue
end


local function runQ1_2_c(keys, args)
    local queriedDates = redis.call("FT.SEARCH", "date-index", "@d_yearmonthnum:[199401 199401]", "RETURN", 1, "d_datekey", "LIMIT", 0, 2147483647)
    table.remove(queriedDates, 1) -- removing the count of results from the table
    local sum_revenue2 = 0
    local query2 = ""
    for k, v in pairs(queriedDates) do
        if v[2] then
            query2 = query2 .. " @lo_orderdate:[" .. v[2] .. " " .. v[2] .. "] |"
        end
    end

    query2 = string.sub(query2, 1, -3) -- Remove the trailing "|"

    --query = query .. " LIMIT 0 2147483647"
    redis.log(redis.LOG_WARNING, "QUERY OF INTEREST...: " .. query2)

    local lineorder_query_result = redis.call("FT.SEARCH", "lineorder-index", "@lo_discount:[4 6] @lo_quantity:[26 35]" .. query2, "LIMIT", 0, 2147483647)
    table.remove(lineorder_query_result, 1) -- removing the count of results from the table
    for x, y in pairs(lineorder_query_result) do
        if y[2] then
           -- redis.log(redis.LOG_WARNING, "y".." | "..y[1].." | "..y[2].." | "..y[3].." | "..y[4])
            --redis.log(redis.LOG_WARNING, "y2: "..y[2].." y4 "..y[4])
            sum_revenue2 = sum_revenue2 + y[2] * y[4]
            --redis.log(redis.LOG_WARNING, sum_revenue)
        end
    end
    --redis.log(redis.LOG_WARNING, "totalDocuemnts "..totalDocuments)
    return "revenue: lol ".. sum_revenue2
end




local function runQ1_1_d(keys, args)
    local lo_orderdate_query_string = redis.call("GET", "yearDateIndex:1993")
    local result = redis.call("FT.AGGREGATE", "lineorder-index", "@lo_discount:[1 3] @lo_quantity:[0 24]"..lo_orderdate_query_string, "LOAD", 2, "@lo_discount", "@lo_extendedprice", "APPLY", "@lo_discount * @lo_extendedprice", "AS", "revenue", "GROUPBY", 0, "REDUCE", "SUM", 1, "@revenue", "AS", "total_revenue")
    return result
end

local function runQ1_2_d(keys, args)
    local queryFilter = queryFilterCriteria(keys, args)
    local result = redis.call("FT.AGGREGATE", "lineorder-index", "@lo_discount:[1 3] @lo_quantity:[0 24]"..queryFilter, "LIMIT", 0 ,2147483647, "GROUPBY", 2, "@lo_discount", "@lo_extendedprice", "LIMIT", 0, 2147483647, "APPLY", "@lo_discount * @lo_extendedprice", "as", "revenue", "LIMIT", 0, 2147483647, "GROUPBY", 0, "REDUCE", "SUM", 1, "@revenue", "as", "total_revenue", "LIMIT", 0, 2147483647)
    return result
end

-- redis.register_function('runQ1_1_b', runQ1_1_b)
-- redis.register_function('runQ1_1_c', runQ1_1_c)
-- redis.register_function('runQ1_1_d', runQ1_1_d)
-- redis.register_function('runQ1_2_b', runQ1_2_b)
-- redis.register_function('runQ1_2_c', runQ1_2_c)
-- redis.register_function('runQ1_2_d', runQ1_2_d)
-- redis.register_function('runQ2_1_a', runQ2_1_a)
]]









