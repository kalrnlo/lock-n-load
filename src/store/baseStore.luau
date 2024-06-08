local budget = require("@src/budget")
local datastoreLinker = require("@src/datastoreLinker")
local jobID, placeID = require("@provided/jobID")()
local key = require("./key")
local keyData = require("./keyData")
local threadQueue = require("./threadQueue")

type UpdateTransformFunction<T> = (keyData.Identity<T>, DataStoreKeyInfo) -> (keyData.Identity<T>, { number }?, {}?)

type RawMetadata = {
	data: {[string]: any},
	session: Session,
}

type Session = {
	timestamp: number, 
	locked: boolean,
	placeid: number,
	jobid: string,
}

type JSONAcceptable = { JSONAcceptable } | { [string]: JSONAcceptable } | number | string | boolean | buffer

type KeyInfo<Template = JSONAcceptable, Metadata = JSONAcceptable?> = {
    metadata: Metadata,
    data: Template,
}

type OrderedKeyInfo<Template = JSONAcceptable, Metadata = JSONAcceptable?> = {
	sort_key: number,
} & KeyInfo<Template, Metadata>

export type Transformer<Template, Params...> = (key_to_modify: Template, Params...) -> ()

type OrderedTransformCreator<Template = JSONAcceptable, Metadata = JSONAcceptable?, Scope = string?> = <Params...>(
	self: OrderedStore<Template, Metadata, Scope>, transformfn: (data: OrderedKeyInfo<Template, Metadata> , Params...) -> (Template, Metadata)
) -> Transformer<Template, Params...>

type TransformCreator<Template = JSONAcceptable, Metadata = JSONAcceptable?, Scope = string?> = <Params...>(
	self: BaseStore<Template, Metadata, Scope>, transformfn: (data: KeyInfo<Template, Metadata>, Params...) -> (Template, Metadata)
) -> Transformer<Template, Params...>

type BaseStore<Template = JSONAcceptable, Metadata = JSONAcceptable?, Scope = string?> = {
	lock_n_load: (self: BaseStore<Template, Metadata, Scope>, key_name: string, userid: number?) -> (),
	view_async: (self: BaseStore<Template, Metadata, Scope>, key_name: string) -> (),
	unlock: (self: BaseStore<Template, Metadata, Scope>, key: unknown) -> (),
	remove: (self: BaseStore<Template, Metadata, Scope>, key: unknown) -> (),

    update_queue: threadQueue.Identity,
    template: Template,
	scope: Scope,
}

export type Store<Template = JSONAcceptable, Metadata = JSONAcceptable?, Scope = string?> = {
	create_global_transformer: TransformCreator<Template, Metadata, Scope>,
	create_async_transformer: TransformCreator<Template, Metadata, Scope>,
    create_transformer: TransformCreator<Template, Metadata, Scope>,

	keys: { [string]: key.Identity<Template> },
	link: datastoreLinker.Identity,
} & BaseStore<Template, Metadata, Scope>

export type OrderedStore<Template = JSONAcceptable, Metadata = JSONAcceptable?, Scope = string?> = {
	create_global_transformer: OrderedTransformCreator<Template, Metadata, Scope>,
	create_async_transformer: OrderedTransformCreator<Template, Metadata, Scope>,
    create_transformer: OrderedTransformCreator<Template, Metadata, Scope>,

	keys: { [string]: key.Identity<Template> },
	link: datastoreLinker.Identity,
} & BaseStore<Template, Metadata, Scope>


local store = {}
export type Identity<T> = typeof(setmetatable(
	{} :: {
		
		_link: datastoreLinker.Identity,
		_keys: { [string]: key.Identity<T> },
		_template: T,
		_threadQueue: threadQueue.Identity,
	},
	{
		__index = store,
	}
))

local stores: { Identity<any> } = {}

local NOOP = function() end

local baseOrderedStore = {

}

local function raw_update_async<T>(store: GlobalDataStore, keyName: string, transformFunction: UpdateTransformFunction<T>)
	return store:UpdateAsync(keyName, transformFunction)
end



local function constructor<T>(name: string, template: T): Identity<T>
	local self = setmetatable({}, { __index = store })

	self._link = datastoreLinker(name)
	self._keys = {}
	self._template = template
	self._threadQueue = threadQueue.new()

	return self
end

function store._update<T>(self: Identity<T>, keyName: string, transformFunction: UpdateTransformFunction<T>)
	self._threadQueue:add(function()
		if not budget.canAfford(false) then
			return false
		end

		local success, err = pcall(raw_update_async, self._link.datastore, keyName, transformFunction)
		if not success then
			warn(err)
		end

		return success
	end, function() end, 5)
end

function store._updateAsync<T>(self: Identity<T>, keyName: string, transformFunction: UpdateTransformFunction<T>): boolean
	local thread = coroutine.running()

	self._threadQueue:add(function()
		if not budget.canAfford(true) then
			return false
		end

		local success, err = pcall(raw_update_async, self._link.datastore, keyName, transformFunction)
		if not success then
			warn(err)
		end

		return success
	end, function(successful)
		task.spawn(thread, successful)
	end, 8)

	return coroutine.yield()
end

function store.lockAndLoad<T>(self: Identity<T>, keyName: string, userID: number?, metadata: { [string]: any }?): key.Identity<T>?
	local potentialKeyData: T? = self._emptyTemplate
    local userIDArray = { userID :: number }

	local successfullyLocked = false
	self:_updateAsync(keyName, function(data, info)
		local newKeyData = keyData.reconcile(data, self._template)
		local metadata: Metadata = info:GetMetadata()
		local session = metadata.session

		local isSessionLocked = session.locked
		local isLockedByUs = session.lockerJobID == jobID and session.lockerPlaceID == placeID
		local isLockExpired = os.time() - session.lockTimestamp >= 60

		local shouldLock = not isSessionLocked -- Case 1: Not locked
			or (isSessionLocked and isLockExpired) -- Case 2: Locked, expired
			or (isLockExpired and isLockedByUs) -- Case 3: Locked, expired, and locked by us

		if shouldLock then
			metadata.session = {
				lockTimestamp = os.time(),
				lockerPlaceID = placeID,
				lockerJobID = jobID,
				locked = true,
			}

			potentialKeyData = self:_extractRawKeyData(newKeyData.data)
			successfullyLocked = true
		end

		return newKeyData, userIDArray, metadata
	end)

	if not successfullyLocked then
		return nil
	end

	local newKey = key.new(keyName, self._template)
	newKey._data = potentialKeyData :: T

	self._keys[keyName] = newKey

	return newKey
end

function store.unlock<T>(self: Identity<T>, keyToUnlock: key.Identity<T>)
	local name = keyToUnlock._name
	self:_updateAsync(name, function(data, info)
		local newKeyData = keyData.reconcile(data, self._template)
		local metadata: Metadata = info:GetMetadata()
		local session = metadata.session

		-- an EVIL server Stole the lock...
		if session.lockerPlaceID ~= placeID and session.lockerJobID ~= jobID then
			return newKeyData, nil, metadata
		end
		
		metadata.session = {
			lockerPlaceID = 0,
			lockTimestamp = 0,
			lockerJobID = "",
			locked = false,
		}


		newKeyData.data = keyToUnlock._data

		return newKeyData, nil, metadata
	end)
end

function store.createTransform<T, A>(_self: Identity<T>, transform: (data: T, A) -> T)
	return function(keyToModify: key.Identity<T>, args: A)
		keyToModify._data = transform(keyToModify._data, args)
		keyToModify._modified = true
	end
end

function store.createAsyncTransform<T, A>(self: Identity<T>, transform: (data: T, A) -> T)
	return function(keyToModify: key.Identity<T>, args: A)
		keyToModify._data = transform(keyToModify._data, args)

		self:_updateAsync(keyToModify._name, function(data, info)
			data.data = keyToModify._data

			return data, info:GetUserIds(), info:GetMetadata()
		end)
	end
end

function store._step<T>(self: Identity<T>)
	for _, memberKey in self._keys do
		if not memberKey._modified and memberKey._lastAutosave <= 30 then
			continue
		end

		if os.time() - memberKey._lastAutosave <= 10 then
			continue
		end

		memberKey._lastAutosave = os.time()
		self:_update(memberKey._name, function(data, info)
			local newKeyData = keyData.reconcile(data, self._template)
			if newKeyData.session.locked and newKeyData.session.lockedBy ~= jobID() then
				(memberKey :: key.Identity<T>):_invokeLockLostCallback()
				return newKeyData, nil, info:GetMetadata()
			end

			newKeyData.session.locked = true
			newKeyData.session.lockedBy = jobID()
			newKeyData.session.lockTimestamp = os.time()

			newKeyData.data = memberKey._data

			return newKeyData, nil, info:GetMetadata()
		end)
	end
end

return {
	step = function()
		for _, storeToStep in stores do
			(storeToStep :: Identity<any>):_step()
		end
	end,

	new = constructor,
}
