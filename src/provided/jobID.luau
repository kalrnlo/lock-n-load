local currentRuntime = require("./currentRuntime")

local placeID = 0
local jobID = ""
if currentRuntime() == "roblox" then
	local isStudio = game:GetService("RunService"):IsStudio()

	jobID = if isStudio then game:GetService("HttpService"):GenerateGUID(false) else game.JobId
	placeID = game.PlaceId
else
	jobID = tostring(math.random(1, 10000))
end

return function(): (string, number)
	return jobID, placeID
end
