--[[
	just an object to describe a "link" to a datastore
]]
local DataStoreService = game:GetService("DataStoreService")

type link = ((name: string, scope: string?, ordered: boolean?) -> Identity) & ((name: string, scope: string?, ordered: true) -> OrderedIdentity)

export type OrderedIdentity = {
	datastore: OrderedDataStore,
}

export type Identity = {
	datastore: GlobalDataStore,
}

local experimental_features = {
	v2 = true,
}

local link = (function(name, scope, ordered)
	if ordered then
		return table.freeze {
			datastore = DataStoreService:GetOrderedDataStore(name, scope :: any),
		} :: any
	else
		local options = Instance.new("DataStoreOptions")
		options:SetExperimentalFeatures(experimental_features)

		return table.freeze {
			datastore = DataStoreService:GetDataStore(name, scope, options),
		} :: any
	end
end) :: link

return link
