--[[
	just an object to describe a "link" to a datastore
]]
local DataStoreService = game:GetService("DataStoreService")

local options = Instance.new("DataStoreOptions")
options:SetExperimentalFeatures({
	v2 = true,
})

type OrderedIdentity = {
	datastore: OrderedDataStore,
	type: "Ordered",
}

type RegularIdentity = {
	datastore: GlobalDataStore,
	type: "Regular",
}

type link = ((name: string, ordered: boolean?) -> RegularIdentity) & ((name: string, ordered: true) -> OrderedIdentity)

local link: link = function(name: string, ordered: boolean?)
	if ordered then
		return table.freeze {
			datastore = DataStoreService:GetOrderedDataStore(name, nil),
			type = "Ordered",
		} :: any
	else
		return table.freeze {
			datastore = DataStoreService:GetDataStore(name, nil, options),
			type = "Regular",
		} :: any
	end
end

export type Identity = typeof(link(""))

return link
