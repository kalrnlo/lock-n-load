local DataStoreService = game:GetService("DataStoreService")

local budget = {}

function budget.canAfford(priority: boolean, request_type: Enum.DataStoreRequestType?)
	local amount = DataStoreService:GetRequestBudgetForRequestType(
		request_type or Enum.DataStoreRequestType.UpdateAsync
	)
	local threshold = if priority then 100 else 5

	return amount >= threshold
end

return budget
