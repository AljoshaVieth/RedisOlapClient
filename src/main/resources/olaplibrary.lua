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
    return redis.call("FT.SEARCH", "date-index", "@d_year:[1993 1993]", "LIMIT", 0, 0)
end

local function queryDocuments(keys, args)
    -- args[1] = index-name
    -- args[2] = query
    -- args[3] = fields to return
    --return args[3]
    return redis.call("FT.SEARCH", args[1], args[2])
end

-- This function is used to extract values of one specific field of queryDocuments
-- args[1] = index-name
-- args[2] = query
-- args[3] = the field to return
local function queryFilterCriteria(keys, args)
    local query_result = redis.call("FT.SEARCH", args[1], args[2], "RETURN", 1, args[3])
    local result = { }
    local flattened = flattenTable(query_result)
    for i = 1, #flattened do
        if flattened[i] == args[3] then
            table.insert(result, flattened[i+1])
        end
    end
    return result
end

local function runQ1_1(keys, args)

end



redis.register_function('querySpecificDocuments', querySpecificDocuments)
redis.register_function('queryDocuments', queryDocuments)
redis.register_function('queryFilterCriteria', queryFilterCriteria)




