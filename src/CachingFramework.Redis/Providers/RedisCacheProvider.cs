﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CachingFramework.Redis.Contracts;
using CachingFramework.Redis.Contracts.Providers;
using StackExchange.Redis;

namespace CachingFramework.Redis.Providers
{
    /// <summary>
    /// Cache provider implementation using Redis.
    /// </summary>
    internal class RedisCacheProvider : RedisProviderBase, ICacheProvider
    {
        #region Constructor
        /// <summary>
        /// Initializes a new instance of the <see cref="RedisCacheProvider"/> class.
        /// </summary>
        /// <param name="context">The context.</param>
        public RedisCacheProvider(RedisProviderContext context)
            : base(context)
        {
        }
        #endregion

        #region Fields
        /// <summary>
        /// The tag format for the keys representing tags
        /// </summary>
        private const string TagFormat = ":$_tag_$:{0}";
        /// <summary>
        /// Separator to use for the value when a tag is related to a HASH field
        /// </summary>
        private const string TagHashSeparator = ":$_->_$:";
        /// <summary>
        /// Separator to use for the value when a tag is related to a SET member
        /// </summary>
        private const string TagSetSeparator = ":$_-S>_$:";
        #endregion

        #region ICacheProviderAsync Implementation

        public async Task<T> FetchObjectAsync<T>(string key, Func<Task<T>> func, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return await FetchObjectAsync(key, func, (string[])null, expiry, flags).ForAwait();
        }

        public async Task<T> FetchObjectAsync<T>(string key, Func<Task<T>> func, string[] tags, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return await FetchObjectAsync(key, func, _ => tags, expiry, flags).ForAwait();
        }

        public async Task<T> FetchObjectAsync<T>(string key, Func<Task<T>> func, Func<T, string[]> tagsBuilder, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            T value = default(T);
            var cacheValue = await RedisConnection.GetDatabase().StringGetAsync(key, flags).ForAwait();
            if (cacheValue.HasValue)
            {
                value = Serializer.Deserialize<T>(cacheValue);
            }
            else
            {
                var task = func.Invoke();
                if (task != null)
                {
                    value = await task;
                    if (value != null)
                    {
                        var tags = tagsBuilder?.Invoke(value);
                        await SetObjectAsync(key, value, tags, expiry, flags: flags).ForAwait();
                    }
                }
            }
            return value;
        }

        public async Task SetObjectAsync<T>(string key, T value, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            var serialized = Serializer.Serialize(value);
            await RedisConnection.GetDatabase().StringSetAsync(key, serialized, ttl, (StackExchange.Redis.When)when, flags).ForAwait();
        }

        public async Task<T> GetSetObjectAsync<T>(string key, T value, CommandFlags flags = CommandFlags.None)
        {
            var serialized = Serializer.Serialize(value);
            var oldValue = await RedisConnection.GetDatabase().StringGetSetAsync(key, serialized, flags).ForAwait();
            if (oldValue.HasValue)
            {
                return Serializer.Deserialize<T>(oldValue);
            }
            return default(T);
        }

        public async Task RenameTagForKeyAsync(string key, string currentTag, string newTag, CommandFlags flags = CommandFlags.None)
        {
            if (currentTag == newTag)
            {
                return;
            }
            var db = RedisConnection.GetDatabase();
            if (await db.SetRemoveAsync(FormatTag(currentTag), key, flags).ForAwait())
            {
                await db.SetAddAsync(FormatTag(newTag), key, flags).ForAwait();
            }
        }

        public async Task RenameTagForHashFieldAsync(string key, string field, string currentTag, string newTag, CommandFlags flags = CommandFlags.None)
        {
            if (currentTag == newTag)
            {
                return;
            }
            var db = RedisConnection.GetDatabase();
            if (await db.SetRemoveAsync(FormatTag(currentTag), FormatHashField(key, field), flags).ForAwait())
            {
                await db.SetAddAsync(FormatTag(newTag), FormatHashField(key, field), flags).ForAwait();
            }
        }

        public async Task RenameTagForSetMemberAsync<T>(string key, T member, string currentTag, string newTag, CommandFlags flags = CommandFlags.None)
        {
            if (currentTag == newTag)
            {
                return;
            }
            var db = RedisConnection.GetDatabase();
            if (await db.SetRemoveAsync(FormatTag(currentTag), FormatSerializedMember(key, TagSetSeparator, member), flags).ForAwait())
            {
                await db.SetAddAsync(FormatTag(newTag), FormatSerializedMember(key, TagSetSeparator, member), flags).ForAwait();
            }
        }

        public async Task<T> GetObjectAsync<T>(string key, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = await RedisConnection.GetDatabase().StringGetAsync(key, flags).ForAwait();
            if (cacheValue.HasValue)
            {
                return Serializer.Deserialize<T>(cacheValue);
            }
            return default(T);
        }

        public async Task<(bool keyExists, T value)> TryGetObjectAsync<T>(string key, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = await RedisConnection.GetDatabase().StringGetAsync(key, flags).ForAwait();
            if (cacheValue.HasValue)
            {
                return (true, Serializer.Deserialize<T>(cacheValue));
            }
            return (false, default(T));
        }

        public async Task<IEnumerable<string>> GetKeysByTagAsync(string[] tags, bool cleanUp = false)
        {
            var db = RedisConnection.GetDatabase();
            ISet<RedisValue> taggedItems;
            if (cleanUp)
            {
                taggedItems = await GetTaggedItemsWithCleanupAsync(db, tags).ForAwait();
            }
            else
            {
                taggedItems = await GetTaggedItemsNoCleanupAsync(db, tags).ForAwait();
            }
            return taggedItems.Select(x => x.ToString());
        }

        public async Task<bool> KeyExistsAsync(string key, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().KeyExistsAsync(key, flags).ForAwait();
        }

        public async Task<bool> KeyExpireAsync(string key, DateTime expiration, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().KeyExpireAsync(key, expiration, flags).ForAwait();
        }

        public async Task<bool> KeyTimeToLiveAsync(string key, TimeSpan ttl, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().KeyExpireAsync(key, ttl, flags).ForAwait();
        }

        public async Task<TimeSpan?> KeyTimeToLiveAsync(string key, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().KeyTimeToLiveAsync(key, flags).ForAwait();
        }

        public async Task<bool> KeyPersistAsync(string key, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().KeyPersistAsync(key, flags).ForAwait();
        }

        public async Task<bool> RemoveAsync(string key, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().KeyDeleteAsync(key, flags).ForAwait();
        }

        public async Task FlushAllAsync(CommandFlags flags = CommandFlags.None)
        {
            RunInAllMasters(async svr => await svr.FlushAllDatabasesAsync(flags).ForAwait());
            await Task.FromResult(0).ForAwait();
        }

        public async Task<bool> HyperLogLogAddAsync<T>(string key, T[] items, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase()
                    .HyperLogLogAddAsync(key, items.Select(x => (RedisValue)Serializer.Serialize(x)).ToArray(), flags).ForAwait();
        }

        public async Task<bool> HyperLogLogAddAsync<T>(string key, T item, CommandFlags flags = CommandFlags.None)
        {
            return await HyperLogLogAddAsync(key, new[] { item }, flags).ForAwait();
        }

        public async Task<long> HyperLogLogCountAsync(string key, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().HyperLogLogLengthAsync(key, flags).ForAwait();
        }

        public async Task InvalidateKeysByTagAsync(params string[] tags)
        {
            var db = RedisConnection.GetDatabase();
            var keys = await GetTaggedItemsNoCleanupAsync(db, tags).ForAwait();
            await InvalidateKeysByTagImplAsync(db, keys, tags).ForAwait();
        }

        public async Task SetObjectAsync<T>(string key, T value, string[] tags, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                await SetObjectAsync(key, value, ttl, when, flags).ForAwait();
                return;
            }
            await SetObjectImplAsync(key, value, tags, ttl, when, flags).ForAwait();
        }
        #endregion 

        #region ICacheProvider Implementation
        /// <summary>
        /// Fetches data from the cache, using the given cache key.
        /// If there is data in the cache with the given key, then that data is returned.
        /// If there is no such data in the cache (a cache miss occurred), then the value returned by func will be
        /// written to the cache under the given cache key and associated to the given tags, and that will be returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The cache key.</param>
        /// <param name="func">The function that returns the cache value, only executed when there is a cache miss.</param>
        /// <param name="expiry">The expiration timespan.</param>
        public T FetchObject<T>(string key, Func<T> func, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return FetchObject(key, func, (string[])null, expiry, flags);
        }
        /// <summary>
        /// Fetches data from the cache, using the given cache key.
        /// If there is data in the cache with the given key, then that data is returned.
        /// If there is no such data in the cache (a cache miss occurred), then the value returned by func will be
        /// written to the cache under the given cache key and associated to the given tags, and that will be returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The cache key.</param>
        /// <param name="func">The function that returns the cache value, only executed when there is a cache miss.</param>
        /// <param name="tags">The tags to associate with the key. Only associated when there is a cache miss.</param>
        /// <param name="expiry">The expiration timespan.</param>
        public T FetchObject<T>(string key, Func<T> func, string[] tags, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return FetchObject(key, func, _ => tags, expiry, flags);
        }

        /// <summary>
        /// Fetches data from the cache, using the given cache key.
        /// If there is data in the cache with the given key, then that data is returned.
        /// If there is no such data in the cache (a cache miss occurred), then the value returned by func will be
        /// written to the cache under the given cache key and associated to the given tags, and that will be returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The cache key.</param>
        /// <param name="func">The function that returns the cache value, only executed when there is a cache miss.</param>
        /// <param name="tagsBuilder">The tag builder to associte tags depending on the value.</param>
        /// <param name="expiry">The expiration timespan.</param>
        public T FetchObject<T>(string key, Func<T> func, Func<T, string[]> tagsBuilder, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            T value;
            if (!TryGetObject(key, out value, flags))
            {
                value = func();
                if (value != null)
                {
                    var tags = tagsBuilder?.Invoke(value);
                    SetObject(key, value, tags, expiry, flags: flags);
                }
            }
            return value;
        }

        /// <summary>
        /// Set the value of a redis string key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="ttl">The expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public void SetObject<T>(string key, T value, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            var serialized = Serializer.Serialize(value);
            RedisConnection.GetDatabase().StringSet(key, serialized, ttl, (StackExchange.Redis.When)when, flags);
        }

        /// <summary>
        /// Set the value of a key, associating the key with the given tag(s).
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="tags">The tags.</param>
        /// <param name="ttl">The expiry.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public void SetObject<T>(string key, T value, string[] tags, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                SetObject(key, value, ttl, when, flags);
                return;
            }
            SetObjectImpl(key, value, tags, ttl, when, flags);
        }

        private void SetObjectImpl<T>(string key, T value, string[] tags, TimeSpan? ttl, Contracts.When when, CommandFlags flags)
        {
            var serialized = Serializer.Serialize(value);
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key relation
                batch.SetAddAsync(tag, key, flags);
                // Set the expiration
                SetMaxExpiration(batch, tag, ttl, flags);
            }
            // Add the key-value
            batch.StringSetAsync(key, serialized, ttl, (StackExchange.Redis.When)when, flags);
            batch.Execute();
        }

        private async Task SetObjectImplAsync<T>(string key, T value, string[] tags, TimeSpan? ttl, Contracts.When when, CommandFlags flags)
        {
            var tasks = new List<Task>();
            var serialized = Serializer.Serialize(value);
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key relation
                tasks.Add(batch.SetAddAsync(tag, key, flags));
                // Set the expiration
                tasks.Add(await SetMaxExpirationAsync(batch, tag, ttl, flags).ForAwait());
            }
            // Add the key-value
            tasks.Add(batch.StringSetAsync(key, serialized, ttl, (StackExchange.Redis.When)when, flags));
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Atomically sets key to value and returns the old value stored at key. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The new value value.</param>
        /// <returns>The old value</returns>
        public T GetSetObject<T>(string key, T value, CommandFlags flags = CommandFlags.None)
        {
            var serialized = Serializer.Serialize(value);
            var oldValue = RedisConnection.GetDatabase().StringGetSet(key, serialized, flags);
            if (oldValue.HasValue)
            {
                return Serializer.Deserialize<T>(oldValue);
            }
            return default(T);
        }
        /// <summary>
        /// Relates the given tags to a key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="tags">The tag(s).</param>
        public void AddTagsToKey(string key, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tag in tags)
            {
                batch.SetAddAsync(FormatTag(tag), key, flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Relates the given tags to a key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="tags">The tag(s).</param>
        public async Task AddTagsToKeyAsync(string key, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tag in tags)
            {
                tasks.Add(batch.SetAddAsync(FormatTag(tag), key, flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Renames a tag related to a key.
        /// If the current tag is not related to the key, no operation is performed.
        /// If the current tag is related to the key, the tag relation is removed and the new tag relation is inserted.
        /// </summary>
        /// <param name="key">The key related to the tag.</param>
        /// <param name="currentTag">The current tag.</param>
        /// <param name="newTag">The new tag.</param>
        public void RenameTagForKey(string key, string currentTag, string newTag, CommandFlags flags = CommandFlags.None)
        {
            if (currentTag == newTag)
            {
                return;
            }
            var db = RedisConnection.GetDatabase();
            if (db.SetRemove(FormatTag(currentTag), key, flags))
            {
                db.SetAdd(FormatTag(newTag), key, flags);
            }
        }

        /// <summary>
        /// Relates the given tags to a field inside a hash key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        /// <param name="tags">The tag(s).</param>
        public void AddTagsToHashField(string key, string field, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tag in tags)
            {
                batch.SetAddAsync(FormatTag(tag), FormatHashField(key, field), flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Relates the given tags to a field inside a hash key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        /// <param name="tags">The tag(s).</param>
        public async Task AddTagsToHashFieldAsync(string key, string field, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tag in tags)
            {
                tasks.Add(batch.SetAddAsync(FormatTag(tag), FormatHashField(key, field), flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        public void AddTagsToSetMember<T>(string key, T member, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tag in tags)
            {
                batch.SetAddAsync(FormatTag(tag), FormatSerializedMember<T>(key, TagSetSeparator, member), flags);
            }
            batch.Execute();
        }

        public async Task AddTagsToSetMemberAsync<T>(string key, T member, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tag in tags)
            {
                tasks.Add(batch.SetAddAsync(FormatTag(tag), FormatSerializedMember<T>(key, TagSetSeparator, member), flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Renames a tag related to a hash field.
        /// If the current tag is not related to the hash field, no operation is performed.
        /// If the current tag is related to the hash field, the tag relation is removed and the new tag relation is inserted.
        /// </summary>
        /// <param name="key">The hash key.</param>
        /// <param name="field">The hash field related to the tag.</param>
        /// <param name="currentTag">The current tag.</param>
        /// <param name="newTag">The new tag.</param>
        public void RenameTagForHashField(string key, string field, string currentTag, string newTag, CommandFlags flags = CommandFlags.None)
        {
            if (currentTag == newTag)
            {
                return;
            }
            var db = RedisConnection.GetDatabase();
            if (db.SetRemove(FormatTag(currentTag), FormatHashField(key, field), flags))
            {
                db.SetAdd(FormatTag(newTag), FormatHashField(key, field), flags);
            }
        }

        public void RenameTagForSetMember<T>(string key, T member, string currentTag, string newTag, CommandFlags flags = CommandFlags.None)
        {
            if (currentTag == newTag)
            {
                return;
            }
            var db = RedisConnection.GetDatabase();
            if (db.SetRemove(FormatTag(currentTag), FormatSerializedMember(key, TagSetSeparator, member), flags))
            {
                db.SetAdd(FormatTag(newTag), FormatSerializedMember(key, TagSetSeparator, member), flags);
            }
        }

        /// <summary>
        /// Removes the relation between the given tags and a field in a hash.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        /// <param name="tags">The tag(s).</param>
        public void RemoveTagsFromHashField(string key, string field, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                batch.SetRemoveAsync(FormatTag(tagName), FormatHashField(key, field), flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Removes the relation between the given tags and a field in a hash.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        /// <param name="tags">The tag(s).</param>
        public async Task RemoveTagsFromHashFieldAsync(string key, string field, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                tasks.Add(batch.SetRemoveAsync(FormatTag(tagName), FormatHashField(key, field), flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Removes the relation between the given tags and a key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="tags">The tag(s).</param>
        public void RemoveTagsFromKey(string key, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                batch.SetRemoveAsync(tag, key, flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Removes the relation between the given tags and a key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="tags">The tag(s).</param>
        public async Task RemoveTagsFromKeyAsync(string key, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                tasks.Add(batch.SetRemoveAsync(tag, key, flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        public void RemoveTagsFromSetMember<T>(string key, T member, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                batch.SetRemoveAsync(FormatTag(tagName), FormatSerializedMember(key, TagSetSeparator, member), flags);
            }
            batch.Execute();
        }

        public async Task RemoveTagsFromSetMemberAsync<T>(string key, T member, string[] tags, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            foreach (var tagName in tags)
            {
                tasks.Add(batch.SetRemoveAsync(FormatTag(tagName), FormatSerializedMember(key, TagSetSeparator, member), flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Removes all the keys and hash fields related to the given tag(s).
        /// </summary>
        /// <param name="tags">The tags.</param>
        public void InvalidateKeysByTag(params string[] tags)
        {
            var db = RedisConnection.GetDatabase();
            var taggedItems = GetTaggedItemsNoCleanup(db, tags);
            InvalidateKeysByTagImpl(db, taggedItems, tags);
        }

        private void InvalidateKeysByTagImpl(IDatabase db, ISet<RedisValue> tagMembers, string[] tags)
        {
            var batch = db.CreateBatch();
            // Delete the keys
            foreach (var tagMember in tagMembers)
            {
                var tmString = tagMember.ToString();
                if (tmString.Contains(TagHashSeparator))
                {
                    // It's a hash field
                    var items = tmString.Split(new[] { TagHashSeparator }, 2, StringSplitOptions.None);
                    var hashKey = items[0];
                    var hashField = GetHashFieldItem(hashKey, tagMember);
                    batch.HashDeleteAsync(hashKey, hashField);
                }
                else if (tmString.Contains(TagSetSeparator))
                {
                    // It's a set member
                    var items = tmString.Split(new[] { TagSetSeparator }, 2, StringSplitOptions.None);
                    var setKey = items[0];
                    byte[] setMember = GetMemberSetItem(setKey, tagMember);
                    var keyType = db.KeyType(setKey);
                    if (keyType == RedisType.SortedSet)
                    {
                        batch.SortedSetRemoveAsync(setKey, setMember);
                    }
                    else
                    {
                        // It's a set or geo index
                        batch.SetRemoveAsync(setKey, setMember);
                    }
                }
                else
                {
                    // It's a string
                    batch.KeyDeleteAsync(tmString);
                }
            }
            // Delete the tags
            foreach (var tagName in tags)
            {
                batch.KeyDeleteAsync(FormatTag(tagName));
            }
            batch.Execute();
        }

        private async Task InvalidateKeysByTagImplAsync(IDatabase db, ISet<RedisValue> tagMembers, string[] tags)
        {
            var tasks = new List<Task>();
            var batch = db.CreateBatch();
            // Delete the keys
            foreach (var tagMember in tagMembers)
            {
                var tmString = tagMember.ToString();
                if (tmString.Contains(TagHashSeparator))
                {
                    // It's a hash field
                    var items = tmString.Split(new[] { TagHashSeparator }, 2, StringSplitOptions.None);
                    var hashKey = items[0];
                    var hashField = GetHashFieldItem(hashKey, tagMember);
                    tasks.Add(batch.HashDeleteAsync(hashKey, hashField));
                }
                else if (tmString.Contains(TagSetSeparator))
                {
                    // It's a set member
                    var items = tmString.Split(new[] { TagSetSeparator }, 2, StringSplitOptions.None);
                    var setKey = items[0];
                    byte[] setMember = GetMemberSetItem(setKey, tagMember);
                    var keyType = await db.KeyTypeAsync(setKey).ForAwait();
                    if (keyType == RedisType.SortedSet)
                    {
                        tasks.Add(batch.SortedSetRemoveAsync(setKey, setMember));
                    }
                    else
                    {
                        // It's a set or geo index
                        tasks.Add(batch.SetRemoveAsync(setKey, setMember));
                    }
                }
                else
                {
                    // It's a string
                    tasks.Add(batch.KeyDeleteAsync(tmString));
                }
            }
            // Delete the tags
            foreach (var tagName in tags)
            {
                tasks.Add(batch.KeyDeleteAsync(FormatTag(tagName)));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Gets all the keys related to the given tag(s).
        /// Returns a hashset with the keys.
        /// Also does the cleanup for the given tags if the parameter cleanUp is true.
        /// Since it is cluster compatible, and cluster does not allow multi-key operations, we cannot use SUNION or LUA scripts.
        /// </summary>
        /// <param name="tags">The tags.</param>
        /// <param name="cleanUp">True to return only the existing keys within the tags (slower). Default is false.</param>
        /// <returns>HashSet{System.String}.</returns>
        public IEnumerable<string> GetKeysByTag(string[] tags, bool cleanUp = false)
        {
            var db = RedisConnection.GetDatabase();
            ISet<RedisValue> taggedItems;
            if (cleanUp)
            {
                taggedItems = GetTaggedItemsWithCleanup(db, tags);
            }
            else
            {
                taggedItems = GetTaggedItemsNoCleanup(db, tags);
            }
            return taggedItems.Select(x => x.ToString());
        }

        /// <summary>
        /// Get all the members related to the given tag
        /// </summary>
        /// <param name="tag">The tag name to get its members</param>
        public IEnumerable<TagMember> GetMembersByTag(string tag)
        {
            var db = RedisConnection.GetDatabase();
            var formatTag = FormatTag(tag);
            if (db.KeyType(formatTag) == RedisType.Set)
            {
                var tagMembers = db.SetMembers(formatTag);
                foreach (var tagMember in tagMembers)
                {
                    var tmString = tagMember.ToString();
                    if (tmString.Contains(TagHashSeparator))
                    {
                        // Hash field
                        var items = tmString.Split(new[] { TagHashSeparator }, 2, StringSplitOptions.None);
                        var hashKey = items[0];
                        yield return new TagMember(Serializer)
                        {
                            Key = hashKey,
                            MemberType = TagMemberType.HashField,
                            MemberValue = GetHashFieldItem(hashKey, tagMember)
                        };
                    }
                    else if (tmString.Contains(TagSetSeparator))
                    {
                        // Set/SortedSet member
                        var items = tmString.Split(new[] { TagSetSeparator }, 2, StringSplitOptions.None);
                        var setKey = items[0];
                        var keyType = db.KeyType(setKey);
                        yield return new TagMember(Serializer)
                        {
                            Key = setKey,
                            MemberType =
                                keyType == RedisType.SortedSet
                                    ? TagMemberType.SortedSetMember
                                    : TagMemberType.SetMember,
                            MemberValue = GetMemberSetItem(setKey, tagMember)
                        };
                    }
                    else
                    {
                        // String
                        yield return new TagMember(null)
                        {
                            Key = tmString,
                            MemberType = TagMemberType.StringKey,
                        };
                    }
                }
            }
        }

        /// <summary>
        /// Returns all the objects that has the given tag(s) related.
        /// Assumes all the objects are of the same type <typeparamref name="T" />.
        /// </summary>
        /// <typeparam name="T">The objects types</typeparam>
        /// <param name="tags">The tags</param>
        public IEnumerable<T> GetObjectsByTag<T>(params string[] tags)
        {
            RedisValue value = default(RedisValue);
            var db = RedisConnection.GetDatabase();
            ISet<RedisValue> tagMembers = GetTaggedItemsNoCleanup(db, tags);
            foreach (var tagMember in tagMembers)
            {
                var tmString = tagMember.ToString();
                if (tmString.Contains(TagHashSeparator))
                {
                    // It's a hash field
                    var items = tmString.Split(new[] { TagHashSeparator }, 2, StringSplitOptions.None);
                    var hashKey = items[0];
                    if (db.KeyType(hashKey) == RedisType.Hash)
                    {
                        var hashField = GetHashFieldItem(hashKey, tagMember);
                        value = db.HashGet(hashKey, hashField);
                    }
                }
                else if (tmString.Contains(TagSetSeparator))
                {
                    // It's a set member
                    var items = tmString.Split(new[] { TagSetSeparator }, 2, StringSplitOptions.None);
                    var setKey = items[0];
                    var keyType = db.KeyType(setKey);
                    if (keyType == RedisType.Set || keyType == RedisType.SortedSet)
                    {
                        //return the member value only if present on set
                        byte[] setMember = GetMemberSetItem(setKey, tagMember);
                        if ((keyType == RedisType.SortedSet && db.SortedSetRank(setKey, setMember).HasValue)
                            ||
                            (keyType == RedisType.Set && db.SetContains(setKey, setMember)))
                        {
                            value = setMember;
                        }
                    }
                }
                else
                {
                    // It's a string
                    if (db.KeyType(tmString) == RedisType.String)
                    {
                        value = db.StringGet(tmString);
                    }
                }
                if (value.HasValue)
                {
                    yield return Serializer.Deserialize<T>(value);
                }
            }
        }
        /// <summary>
        /// Gets a deserialized value from a key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <returns>``0.</returns>
        public T GetObject<T>(string key, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = RedisConnection.GetDatabase().StringGet(key, flags);
            if (cacheValue.HasValue)
            {
                return Serializer.Deserialize<T>(cacheValue);
            }
            return default;
        }
        /// <summary>
        /// Try to get the value of a key
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value"> When this method returns, contains the value associated with the specified key, if the key is found; 
        /// otherwise, the default value for the type of the value parameter.</param>
        /// <returns>True if the cache contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetObject<T>(string key, out T value, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = RedisConnection.GetDatabase().StringGet(key, flags);
            if (!cacheValue.HasValue)
            {
                value = default(T);
                return false;
            }
            value = Serializer.Deserialize<T>(cacheValue);
            return true;
        }
        /// <summary>
        /// Returns the entire collection of tags
        /// </summary>
        public IEnumerable<string> GetAllTags()
        {
            int startIndex = string.Format(TagFormat, "").Length;
            return
                EnumerateInAllMasters(svr => svr.Keys(RedisConnection.GetDatabase().Database, string.Format(TagFormat, "*")))
                    .SelectMany(run => run.Select(r => r.ToString().Substring(startIndex)));
        }

        /// <summary>
        /// Return the keys that matches a specified pattern.
        /// Will use SCAN or KEYS depending on the server capabilities.
        /// </summary>
        /// <param name="pattern">The glob-style pattern to match</param>
        public IEnumerable<string> GetKeysByPattern(string pattern, CommandFlags flags = CommandFlags.None)
        {
            return
                EnumerateInAllMasters(svr => svr.Keys(RedisConnection.GetDatabase().Database, pattern, flags: flags))
                    .SelectMany(
                        run => run.Select(r => r.ToString()).Where(key => !key.StartsWith(string.Format(TagFormat, ""))));
        }

        /// <summary>
        /// Removes the specified key-value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        /// <remarks>Redis command: DEL key</remarks>
        public bool Remove(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().KeyDelete(key, flags);
        }
        /// <summary>
        /// Determines if a key exists.
        /// </summary>
        /// <param name="key">The key.</param>
        public bool KeyExists(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().KeyExists(key, flags);
        }
        /// <summary>
        /// Sets the expiration of a key from a local date time expiration value.
        /// </summary>
        /// <param name="key">The key to expire</param>
        /// <param name="expiration">The expiration local date time</param>
        /// <returns>True if the key expiration was updated</returns>
        public bool KeyExpire(string key, DateTime expiration, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().KeyExpire(key, expiration, flags);
        }
        /// <summary>
        /// Sets the time-to-live of a key from a timespan value.
        /// </summary>
        /// <param name="key">The key to expire</param>
        /// <param name="ttl">The TTL timespan</param>
        /// <returns>True if the key expiration was updated</returns>
        public bool KeyTimeToLive(string key, TimeSpan ttl, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().KeyExpire(key, ttl, flags);
        }

        /// <summary>
        /// Sets the time-to-live of a key from a timespan value, also updates the TTL for the given tags.
        /// </summary>
        /// <param name="key">The key to expire</param>
        /// <param name="ttl">The TTL timespan</param>
        /// <param name="tags">The tags to apply the TTL</param>
        public void KeyTimeToLive(string key, string[] tags, TimeSpan ttl, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                KeyTimeToLive(key, ttl, flags);
                return;
            }
            var batch = RedisConnection.GetDatabase().CreateBatch();
            batch.KeyExpireAsync(key, ttl, flags);
            foreach(var tagName in tags)
            {
                var tag = FormatTag(tagName);
                SetMaxExpiration(batch, tag, ttl, flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Sets the time-to-live of a key from a timespan value, also updates the TTL for the given tags.
        /// </summary>
        /// <param name="key">The key to expire</param>
        /// <param name="ttl">The TTL timespan</param>
        /// <param name="tags">The tags to apply the TTL</param>
        public async Task KeyTimeToLiveAsync(string key, string[] tags, TimeSpan ttl, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                await KeyTimeToLiveAsync(key, ttl, flags);
                return;
            }
            var tasks = new List<Task>();
            var batch = RedisConnection.GetDatabase().CreateBatch();
            tasks.Add(batch.KeyExpireAsync(key, ttl, flags));
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                tasks.Add(await SetMaxExpirationAsync(batch, tag, ttl, flags).ForAwait());
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Gets the time-to-live of a key.
        /// Returns NULL when key does not exist or does not have a timeout.
        /// </summary>
        /// <param name="key">The redis key to get its time-to-live</param>
        public TimeSpan? KeyTimeToLive(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().KeyTimeToLive(key, flags);
        }
        /// <summary>
        /// Removes the expiration of the given key.
        /// </summary>
        /// <param name="key">The key to persist</param>
        /// <returns>True is the key expiration was removed</returns>
        public bool KeyPersist(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().KeyPersist(key, flags);
        }
        /// <summary>
        /// Removes the specified keys.
        /// </summary>
        /// <param name="keys">The keys to remove.</param>
        public void Remove(string[] keys, CommandFlags flags = CommandFlags.None)
        {
            var batch = RedisConnection.GetDatabase().CreateBatch();
            foreach (var key in keys)
            {
                batch.KeyDeleteAsync(key, flags);
            }
            batch.Execute();
        }
        /// <summary>
        /// Removes the specified keys.
        /// </summary>
        /// <param name="keys">The keys to remove.</param>
        public async Task RemoveAsync(string[] keys, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var batch = RedisConnection.GetDatabase().CreateBatch();
            foreach (var key in keys)
            {
                tasks.Add(batch.KeyDeleteAsync(key, flags));
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        public void AddToSet<T>(string key, T value, string[] tags = null, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            batch.SetAddAsync(key, Serializer.Serialize(value), flags);
            // Set the key expiration
            SetMaxExpiration(batch, key, ttl, flags);
            if (tags != null)
            {
                foreach (var tagName in tags)
                {
                    var tag = FormatTag(tagName);
                    // Add the tag-key->field relation
                    batch.SetAddAsync(tag, FormatSerializedMember(key, TagSetSeparator, value), flags);
                    // Set the tag expiration
                    SetMaxExpiration(batch, tag, ttl, flags);
                }
            }
            batch.Execute();
        }

        public async Task AddToSetAsync<T>(string key, T value, string[] tags = null, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            tasks.Add(batch.SetAddAsync(key, Serializer.Serialize(value), flags));
            // Set the key expiration
            tasks.Add(await SetMaxExpirationAsync(batch, key, ttl, flags).ForAwait());
            if (tags != null)
            {
                foreach (var tagName in tags)
                {
                    var tag = FormatTag(tagName);
                    // Add the tag-key->field relation
                    tasks.Add(batch.SetAddAsync(tag, FormatSerializedMember(key, TagSetSeparator, value), flags));
                    // Set the tag expiration
                    tasks.Add(await SetMaxExpirationAsync(batch, tag, ttl, flags).ForAwait());
                }
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        public bool RemoveFromSet<T>(string key, T value, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            return db.SetRemove(key, Serializer.Serialize(value), flags);
        }

        public void AddToSortedSet<T>(string key, double score, T value, string[] tags = null, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            batch.SortedSetAddAsync(key, Serializer.Serialize(value), score, flags);
            // Set the key expiration
            SetMaxExpiration(batch, key, ttl, flags);
            if (tags != null)
            {
                foreach (var tagName in tags)
                {
                    var tag = FormatTag(tagName);
                    // Add the tag-key->field relation
                    batch.SetAddAsync(tag, FormatSerializedMember(key, TagSetSeparator, value), flags);
                    // Set the tag expiration
                    SetMaxExpiration(batch, tag, ttl, flags);
                }
            }
            batch.Execute();
        }

        public async Task AddToSortedSetAsync<T>(string key, double score, T value, string[] tags = null, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            tasks.Add(batch.SortedSetAddAsync(key, Serializer.Serialize(value), score, flags));
            // Set the key expiration
            tasks.Add(await SetMaxExpirationAsync(batch, key, ttl, flags).ForAwait());
            if (tags != null)
            {
                foreach (var tagName in tags)
                {
                    var tag = FormatTag(tagName);
                    // Add the tag-key->field relation
                    tasks.Add(batch.SetAddAsync(tag, FormatSerializedMember(key, TagSetSeparator, value), flags));
                    // Set the tag expiration
                    tasks.Add(await SetMaxExpirationAsync(batch, tag, ttl, flags).ForAwait());
                }
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        public bool RemoveFromSortedSet<T>(string key, T value, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            return db.SortedSetRemove(key, Serializer.Serialize(value), flags);
        }

        public async Task<bool> RemoveFromSortedSetAsync<T>(string key, T value, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            return await db.SortedSetRemoveAsync(key, Serializer.Serialize(value), flags).ForAwait();
        }

        /// <summary>
        /// Fetches hashed data from the cache, using the given cache key and field.
        /// If there is data in the cache with the given key, then that data is returned.
        /// If there is no such data in the cache (a cache miss occurred), then the value returned by func will be
        /// written to the cache under the given cache key-field, and that will be returned.
        /// </summary>
        /// <param name="key">The cache key.</param>
        /// <param name="field">The field to obtain.</param>
        /// <param name="func">The function that returns the cache value, only executed when there is a cache miss.</param>
        /// <param name="expiry">The expiration timespan.</param>
        public T FetchHashed<T>(string key, string field, Func<T> func, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return FetchHashed(key, field, func, (string[])null, expiry, flags);
        }
        /// <summary>
        /// Fetches hashed data from the cache, using the given cache key and field, and associates the field to the given tags.
        /// If there is data in the cache with the given key, then that data is returned, and the last three parameters are ignored.
        /// If there is no such data in the cache (a cache miss occurred), then the value returned by func will be
        /// written to the cache under the given cache key-field, and that will be returned.
        /// </summary>
        /// <param name="key">The cache key.</param>
        /// <param name="field">The field to obtain.</param>
        /// <param name="func">The function that returns the cache value, only executed when there is a cache miss.</param>
        /// <param name="tags">The tags to relate to this field.</param>
        /// <param name="expiry">The expiration timespan.</param>
        public T FetchHashed<T>(string key, string field, Func<T> func, string[] tags, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return FetchHashed(key, field, func, _ => tags, expiry, flags);
        }

        /// <summary>
        /// Fetches hashed data from the cache, using the given cache key and field, and associates the field to the tags returned by the given tag builder.
        /// If there is data in the cache with the given key, then that data is returned, and the last three parameters are ignored.
        /// If there is no such data in the cache (a cache miss occurred), then the value returned by func will be
        /// written to the cache under the given cache key-field, and that will be returned.
        /// </summary>
        /// <param name="key">The cache key.</param>
        /// <param name="field">The field to obtain.</param>
        /// <param name="func">The function that returns the cache value, only executed when there is a cache miss.</param>
        /// <param name="tagsBuilder">The tag builder to specify tags depending on the value.</param>
        /// <param name="expiry">The expiration timespan.</param>
        public T FetchHashed<T>(string key, string field, Func<T> func, Func<T, string[]> tagsBuilder, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            T value;
            if (!TryGetHashed(key, field, out value, flags))
            {
                value = func();
                // ReSharper disable once CompareNonConstrainedGenericWithNull
                if (value != null)
                {
                    var tags = tagsBuilder?.Invoke(value);
                    SetHashed(key, field, value, tags, expiry, flags: flags);
                }
            }
            return value;
        }

        public TV FetchHashed<TK, TV>(string key, TK field, Func<TV> func, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return FetchHashed(key, field, func, (string[])null, expiry, flags);
        }

        public TV FetchHashed<TK, TV>(string key, TK field, Func<TV> func, string[] tags, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return FetchHashed(key, field, func, _ => tags, expiry, flags);
        }

        public TV FetchHashed<TK, TV>(string key, TK field, Func<TV> func, Func<TV, string[]> tagsBuilder, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            TV value;
            if (!TryGetHashed(key, field, out value, flags))
            {
                value = func();
                // ReSharper disable once CompareNonConstrainedGenericWithNull
                if (value != null)
                {
                    var tags = tagsBuilder?.Invoke(value);
                    SetHashed(key, field, value, tags, expiry, flags: flags);
                }
            }
            return value;
        }

        public async Task<T> FetchHashedAsync<T>(string key, string field, Func<Task<T>> func, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return await FetchHashedAsync(key, field, func, (string[])null, expiry, flags).ForAwait();
        }

        public async Task<T> FetchHashedAsync<T>(string key, string field, Func<Task<T>> func, string[] tags, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return await FetchHashedAsync(key, field, func, _ => tags, expiry, flags).ForAwait();
        }

        public async Task<T> FetchHashedAsync<T>(string key, string field, Func<Task<T>> func, Func<T, string[]> tagsBuilder, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            T value = default(T);
            var cacheValue = await RedisConnection.GetDatabase().HashGetAsync(key, field, flags).ForAwait();
            if (cacheValue.HasValue)
            {
                value = Serializer.Deserialize<T>(cacheValue);
            }
            else
            {
                var task = func.Invoke();
                if (task != null)
                {
                    value = await task.ForAwait();
                    if (value != null)
                    {
                        var tags = tagsBuilder?.Invoke(value);
                        await SetHashedAsync(key, field, value, tags, expiry, flags: flags).ForAwait();
                    }
                }
            }
            return value;
        }

        public async Task<TV> FetchHashedAsync<TK, TV>(string key, TK field, Func<Task<TV>> func, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return await FetchHashedAsync<TK, TV>(key, field, func, (string[])null, expiry, flags).ForAwait();
        }

        public async Task<TV> FetchHashedAsync<TK, TV>(string key, TK field, Func<Task<TV>> func, string[] tags, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            return await FetchHashedAsync(key, field, func, _ => tags, expiry, flags).ForAwait();
        }

        public async Task<TV> FetchHashedAsync<TK, TV>(string key, TK field, Func<Task<TV>> func, Func<TV, string[]> tagsBuilder, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            TV value = default(TV);
            var cacheValue = await RedisConnection.GetDatabase().HashGetAsync(key, Serializer.Serialize(field), flags).ForAwait();
            if (cacheValue.HasValue)
            {
                value = Serializer.Deserialize<TV>(cacheValue);
            }
            else
            {
                var task = func.Invoke();
                if (task != null)
                {
                    value = await task.ForAwait();
                    if (value != null)
                    {
                        var tags = tagsBuilder?.Invoke(value);
                        await SetHashedAsync(key, field, value, tags, expiry, flags: flags).ForAwait();
                    }
                }
            }
            return value;
        }

        public async Task<T> GetHashedAsync<T>(string key, string field, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = await RedisConnection.GetDatabase().HashGetAsync(key, field, flags).ForAwait();
            return cacheValue.HasValue ? Serializer.Deserialize<T>(cacheValue) : default(T);
        }

        public async Task<TV> GetHashedAsync<TK, TV>(string key, TK field, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = await RedisConnection.GetDatabase().HashGetAsync(key, Serializer.Serialize(field), flags).ForAwait();
            return cacheValue.HasValue ? Serializer.Deserialize<TV>(cacheValue) : default(TV);
        }

        /// <summary>
        /// Removes a specified hased value from cache
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        public bool RemoveHashed(string key, string field, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().HashDelete(key, field, flags);
        }

        public bool RemoveHashed<TK>(string key, TK field, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().HashDelete(key, Serializer.Serialize(field), flags);
        }

        public async Task<bool> RemoveHashedAsync(string key, string field, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().HashDeleteAsync(key, field, flags).ForAwait();
        }

        public async Task<bool> RemoveHashedAsync<TK>(string key, TK field, CommandFlags flags = CommandFlags.None)
        {
            return await RedisConnection.GetDatabase().HashDeleteAsync(key, Serializer.Serialize(field), flags).ForAwait();
        }

        public async Task<IDictionary<string, T>> GetHashedAllAsync<T>(string key, CommandFlags flags = CommandFlags.None)
        {
            var hashValues = await RedisConnection.GetDatabase().HashGetAllAsync(key, flags).ForAwait();
            return hashValues.ToDictionary(k => k.Name.ToString(), v => Serializer.Deserialize<T>(v.Value));
        }

        public async Task<IDictionary<TK, TV>> GetHashedAllAsync<TK, TV>(string key, CommandFlags flags = CommandFlags.None)
        {
            var hashValues = await RedisConnection.GetDatabase().HashGetAllAsync(key, flags).ForAwait();
            return hashValues.ToDictionary(k => Serializer.Deserialize<TK>(k.Name), v => Serializer.Deserialize<TV>(v.Value));
        }

        /// <summary>
        /// Gets all the values from a hash, assuming all the values in the hash are of the same type <typeparamref name="T" />.
        /// The keys of the dictionary are the field names and the values are the objects.
        /// The fields are assumed to be plain strings, otherwise use the overload indicating the field type to deserialize to.
        /// </summary>
        /// <typeparam name="T">The value type</typeparam>
        /// <param name="key">The redis key.</param>
        public IDictionary<string, T> GetHashedAll<T>(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase()
                .HashGetAll(key, flags)
                .ToDictionary(k => k.Name.ToString(), v => Serializer.Deserialize<T>(v.Value));
        }

        public IDictionary<TK, TV> GetHashedAll<TK, TV>(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase()
                .HashGetAll(key, flags)
                .ToDictionary(k => Serializer.Deserialize<TK>(k.Name), v => Serializer.Deserialize<TV>(v.Value));
        }


        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public void SetHashed<T>(string key, string field, T value, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            batch.HashSetAsync(key, field, Serializer.Serialize(value), (StackExchange.Redis.When)when, flags);
            // Set the key expiration
            SetMaxExpiration(batch, key, ttl, flags);
            batch.Execute();
        }

        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public void SetHashed<TK, TV>(string key, TK field, TV value, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            batch.HashSetAsync(key, Serializer.Serialize(field), Serializer.Serialize(value), (StackExchange.Redis.When)when, flags);
            // Set the key expiration
            SetMaxExpiration(batch, key, ttl, flags);
            batch.Execute();
        }

        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="tags">The tags to relate to this field.</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public void SetHashed<T>(string key, string field, T value, string[] tags, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                SetHashed(key, field, value, ttl, when, flags);
                return;
            }
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            batch.HashSetAsync(key, field, Serializer.Serialize(value), (StackExchange.Redis.When)when, flags);
            // Set the key expiration
            SetMaxExpiration(batch, key, ttl, flags);
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key->field relation
                batch.SetAddAsync(tag, FormatHashField(key, field), flags);
                // Set the tag expiration
                SetMaxExpiration(batch, tag, ttl, flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="tags">The tags to relate to this field.</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public void SetHashed<TK, TV>(string key, TK field, TV value, string[] tags, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                SetHashed(key, field, value, ttl, when, flags);
                return;
            }
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            batch.HashSetAsync(key, Serializer.Serialize(field), Serializer.Serialize(value), (StackExchange.Redis.When)when, flags);
            // Set the key expiration
            SetMaxExpiration(batch, key, ttl, flags);
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key->field relation
                batch.SetAddAsync(tag, FormatSerializedMember(key, TagHashSeparator, field), flags);
                // Set the tag expiration
                SetMaxExpiration(batch, tag, ttl, flags);
            }
            batch.Execute();
        }

        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public async Task SetHashedAsync<T>(string key, string field, T value, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            tasks.Add(batch.HashSetAsync(key, field, Serializer.Serialize(value), (StackExchange.Redis.When)when, flags));
            if (ttl.HasValue)
            {
                tasks.Add(await SetMaxExpirationAsync(batch, key, ttl, flags).ForAwait());
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }
        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="TK">The type of the key field</typeparam>
        /// <typeparam name="TV">The value type</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public async Task SetHashedAsync<TK, TV>(string key, TK field, TV value, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            tasks.Add(batch.HashSetAsync(key, Serializer.Serialize(field), Serializer.Serialize(value), (StackExchange.Redis.When)when, flags));
            if (ttl.HasValue)
            {
                tasks.Add(await SetMaxExpirationAsync(batch, key, ttl, flags).ForAwait());
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="tags">The tags to relate to this field.</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public async Task SetHashedAsync<T>(string key, string field, T value, string[] tags, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                await SetHashedAsync(key, field, value, ttl, when, flags).ForAwait();
                return;
            }
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            tasks.Add(batch.HashSetAsync(key, field, Serializer.Serialize(value), (StackExchange.Redis.When)when, flags));
            if (ttl.HasValue)
            {
                tasks.Add(await SetMaxExpirationAsync(batch, key, ttl, flags).ForAwait());
            }
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key->field relation
                tasks.Add(batch.SetAddAsync(tag, FormatHashField(key, field), flags));
                // Set the tag expiration
                if (ttl.HasValue)
                {
                    tasks.Add(await SetMaxExpirationAsync(batch, tag, ttl, flags).ForAwait());
                }
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Sets the specified value to a hashset using the pair hashKey+field.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field key</param>
        /// <param name="value">The value to store</param>
        /// <param name="tags">The tags to relate to this field.</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this hash). NULL to keep the current expiration.</param>
        /// <param name="when">Indicates when this operation should be performed.</param>
        public async Task SetHashedAsync<TK, TV>(string key, TK field, TV value, string[] tags, TimeSpan? ttl = null, Contracts.When when = Contracts.When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                await SetHashedAsync(key, field, value, ttl, when, flags).ForAwait();
                return;
            }
            var tasks = new List<Task>();
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            tasks.Add(batch.HashSetAsync(key, Serializer.Serialize(field), Serializer.Serialize(value), (StackExchange.Redis.When)when, flags));
            // Set the key expiration
            if (ttl.HasValue)
            {
                tasks.Add(await SetMaxExpirationAsync(batch, key, ttl, flags).ForAwait());
            }
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key->field relation
                tasks.Add(batch.SetAddAsync(tag, FormatSerializedMember(key, TagHashSeparator, field), flags));
                // Set the tag expiration
                if (ttl.HasValue)
                {
                    tasks.Add(await SetMaxExpirationAsync(batch, tag, ttl, flags).ForAwait());
                }
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Sets the specified key/values pairs to a hashset.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fieldValues">The field keys and values to store</param>
        public async Task SetHashedAsync<T>(string key, IDictionary<string, T> fieldValues)
        {
            var db = RedisConnection.GetDatabase();
            await db.HashSetAsync(key,
                fieldValues
                    .Select(x => new HashEntry(x.Key, Serializer.Serialize(x.Value)))
                    .ToArray()).ForAwait();
        }

        /// <summary>
        /// Sets the specified key/values pairs to a hashset.
        /// (The latest expiration applies to the whole key)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fieldValues">The field keys and values to store</param>
        public void SetHashed<T>(string key, IDictionary<string, T> fieldValues)
        {
            var db = RedisConnection.GetDatabase();
            var fields = fieldValues.Select(x => new HashEntry(x.Key, Serializer.Serialize(x.Value))).ToArray();
            db.HashSet(key, fields);
        }

        /// <summary>
        /// Sets multiple values to the hashset stored on the given key.
        /// The field can be any serializable type
        /// </summary>
        /// <typeparam name="TK">The field type</typeparam>
        /// <typeparam name="TV">The value type</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fieldValues">The field keys and values</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this field). NULL to keep the current expiration.</param>
        public void SetHashed<TK, TV>(string key, IEnumerable<KeyValuePair<TK, TV>> fieldValues, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var fields = fieldValues.Select(x => new HashEntry(Serializer.Serialize(x.Key), Serializer.Serialize(x.Value))).ToArray();
            db.HashSet(key, fields, flags);
        }

        public void SetHashed<TK, TV>(string key, IEnumerable<KeyValuePair<TK, TV>> fieldValues, string[] tags, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                SetHashed(key, fieldValues, ttl, flags);
                return;
            }
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            var fields = fieldValues.Select(x => new HashEntry(Serializer.Serialize(x.Key), Serializer.Serialize(x.Value))).ToArray();
            batch.HashSetAsync(key, fields, flags);
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key->field relations
                foreach (var fieldValue in fieldValues)
                {
                    batch.SetAddAsync(tag, FormatSerializedMember(key, TagHashSeparator, fieldValue.Key), flags);
                }
            }
            batch.Execute();
        }

        public async Task SetHashedAsync<TK, TV>(string key, IEnumerable<KeyValuePair<TK, TV>> fieldValues, string[] tags, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            if (tags == null || tags.Length == 0)
            {
                await SetHashedAsync(key, fieldValues, ttl, flags).ForAwait();
                return;
            }
            var db = RedisConnection.GetDatabase();
            var batch = db.CreateBatch();
            var fields = fieldValues.Select(x => new HashEntry(Serializer.Serialize(x.Key), Serializer.Serialize(x.Value))).ToArray();
            var tasks = new List<Task>();
            tasks.Add(batch.HashSetAsync(key, fields, flags));
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                // Add the tag-key->field relations
                foreach (var fieldValue in fieldValues)
                {
                    tasks.Add(batch.SetAddAsync(tag, FormatSerializedMember(key, TagHashSeparator, fieldValue.Key), flags));
                }
            }
            batch.Execute();
            await Task.WhenAll(tasks).ForAwait();
        }

        /// <summary>
        /// Sets multiple values to the hashset stored on the given key.
        /// The field can be any serializable type
        /// </summary>
        /// <typeparam name="TK">The field type</typeparam>
        /// <typeparam name="TV">The value type</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fieldValues">The field keys and values</param>
        /// <param name="ttl">Set the current expiration timespan to the whole key (not only this field). NULL to keep the current expiration.</param>
        public async Task SetHashedAsync<TK, TV>(string key, IEnumerable<KeyValuePair<TK, TV>> fieldValues, TimeSpan? ttl = null, CommandFlags flags = CommandFlags.None)
        {
            var db = RedisConnection.GetDatabase();
            var fields = fieldValues.Select(x => new HashEntry(Serializer.Serialize(x.Key), Serializer.Serialize(x.Value))).ToArray();
            await db.HashSetAsync(key, fields, flags).ForAwait();
        }

        /// <summary>
        /// Gets a specified hashed value from a key and a field
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        public T GetHashed<T>(string key, string field, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = RedisConnection.GetDatabase().HashGet(key, field, flags);
            return cacheValue.HasValue ? Serializer.Deserialize<T>(cacheValue) : default(T);
        }

        /// <summary>
        /// Gets a specified hashed value from a key and field
        /// </summary>
        /// <typeparam name="TK">The type of the hash fields</typeparam>
        /// <typeparam name="TV">The type of the hash values</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        public TV GetHashed<TK, TV>(string key, TK field, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = RedisConnection.GetDatabase().HashGet(key, Serializer.Serialize(field), flags);
            return cacheValue.HasValue ? Serializer.Deserialize<TV>(cacheValue) : default(TV);
        }

        /// <summary>
        /// Gets the specified hashed values from an array of hash fields of type string
        /// </summary>
        /// <typeparam name="TV">The type of the hash values</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fields">The fields to get.</param>
        public TV[] GetHashed<TV>(string key, params string[] fields)
        {
            if (fields == null || fields.Length == 0)
            {
                return Array.Empty<TV>();
            }
            var hashFields = fields.Select(x => (RedisValue)x).ToArray();
            var cacheValues = RedisConnection.GetDatabase().HashGet(key, hashFields);
            return cacheValues.Select(v => v.HasValue ? Serializer.Deserialize<TV>(v) : default(TV)).ToArray();
        }

        /// <summary>
        /// Gets the specified hashed values from an array of hash fields
        /// </summary>
        /// <typeparam name="TK">The type of the hash fields</typeparam>
        /// <typeparam name="TV">The type of the hash values</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fields">The fields.</param>
        public TV[] GetHashed<TK, TV>(string key, params TK[] fields)
        {
            if (fields == null || fields.Length == 0)
            {
                return Array.Empty<TV>();
            }
            var hashFields = fields.Select(f => (RedisValue)Serializer.Serialize(f)).ToArray();
            var cacheValues = RedisConnection.GetDatabase().HashGet(key, hashFields);
            return cacheValues.Select(v => v.HasValue ? Serializer.Deserialize<TV>(v) : default(TV)).ToArray();
        }

        /// <summary>
        /// Asynchronously gets the specified hashed values from an array of hash fields
        /// </summary>
        /// <typeparam name="TK">The type of the hash fields</typeparam>
        /// <typeparam name="TV">The type of the hash values</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fields">The fields.</param>
        public async Task<TV[]> GetHashedAsync<TK, TV>(string key, params TK[] fields)
        {
            if (fields == null || fields.Length == 0)
            {
                return Array.Empty<TV>();
            }
            var hashFields = fields.Select(f => (RedisValue)Serializer.Serialize(f)).ToArray();
            var cacheValues = await RedisConnection.GetDatabase().HashGetAsync(key, hashFields).ForAwait();
            return cacheValues.Select(v => v.HasValue ? Serializer.Deserialize<TV>(v) : default(TV)).ToArray();
        }

        /// <summary>
        /// Asynchronously gets the specified hashed values from an array of hash fields of type string
        /// </summary>
        /// <typeparam name="TV">The type of the hash values</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="fields">The fields to get.</param>
        public async Task<TV[]> GetHashedAsync<TV>(string key, params string[] fields)
        {
            if (fields == null || fields.Length == 0)
            {
                return Array.Empty<TV>();
            }
            var hashFields = fields.Select(x => (RedisValue)x).ToArray();
            var cacheValues = await RedisConnection.GetDatabase().HashGetAsync(key, hashFields).ForAwait();
            return cacheValues.Select(v => v.HasValue ? Serializer.Deserialize<TV>(v) : default(TV)).ToArray();
        }

        /// <summary>
        /// Try to get the value of an element in a hashed key
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The hash field.</param>
        /// <param name="value">When this method returns, contains the value associated with the specified hash field within the key, if the key and field are found; 
        /// otherwise, the default value for the type of the value parameter.</param>
        /// <returns>True if the cache contains a hashed element with the specified key and field; otherwise, false.</returns>
        public bool TryGetHashed<T>(string key, string field, out T value, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = RedisConnection.GetDatabase().HashGet(key, field, flags);
            if (!cacheValue.HasValue)
            {
                value = default(T);
                return false;
            }
            value = Serializer.Deserialize<T>(cacheValue);
            return true;
        }

        public bool TryGetHashed<TK, TV>(string key, TK field, out TV value, CommandFlags flags = CommandFlags.None)
        {
            var cacheValue = RedisConnection.GetDatabase().HashGet(key, Serializer.Serialize(field), flags);
            if (!cacheValue.HasValue)
            {
                value = default(TV);
                return false;
            }
            value = Serializer.Deserialize<TV>(cacheValue);
            return true;
        }

        /// <summary>
        /// Matches a pattern on the field name of a hash, returning its values, assuming all the values in the hash are of the same type <typeparamref name="T" />.
        /// The keys of the dictionary are the field names and the values are the objects
        /// </summary>
        /// <typeparam name="T">The field value type</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="pattern">The glob-style pattern to match.</param>
        /// <param name="pageSize">The scan page size to use.</param>
        public IEnumerable<KeyValuePair<string, T>> ScanHashed<T>(string key, string pattern, int pageSize = 10, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase().HashScan(key, pattern, pageSize, flags)
                .Select(x => new KeyValuePair<string, T>(x.Name, Serializer.Deserialize<T>(x.Value)));
        }

        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the specified key.
        /// </summary>
        /// <typeparam name="T">The items type</typeparam>
        /// <param name="key">The redis key.</param>
        /// <param name="items">The items to add.</param>
        /// <returns><c>true</c> if at least 1 HyperLogLog internal register was altered, <c>false</c> otherwise.</returns>
        public bool HyperLogLogAdd<T>(string key, T[] items, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase()
                    .HyperLogLogAdd(key, items.Select(x => (RedisValue) Serializer.Serialize(x)).ToArray(), flags);
        }
        /// <summary>
        /// Adds the element to the HyperLogLog data structure stored at the specified key.
        /// </summary>
        /// <typeparam name="T">The items type</typeparam>
        /// <param name="key">The redis key.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><c>true</c> if at least 1 HyperLogLog internal register was altered, <c>false</c> otherwise.</returns>
        public bool HyperLogLogAdd<T>(string key, T item, CommandFlags flags = CommandFlags.None)
        {
            return HyperLogLogAdd(key, new[] { item }, flags);
        }
        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key, which is 0 if the variable does not exist.
        /// </summary>
        /// <param name="key">The redis key.</param>
        /// <returns>System.Int64.</returns>
        public long HyperLogLogCount(string key, CommandFlags flags = CommandFlags.None)
        {
            return RedisConnection.GetDatabase()
                    .HyperLogLogLength(key, flags);
        }
        /// <summary>
        /// Flushes all the databases on every master node.
        /// </summary>
        public void FlushAll(CommandFlags flags = CommandFlags.None)
        {
            RunInAllMasters(svr => svr.FlushAllDatabases(flags));
        }

        /// <inheritdoc />
        public bool IsStringKeyInTag(string key, params string[] tags)
        {
            if (tags == null || tags.Length == 0)
            {
                return false;
            }
            var db = RedisConnection.GetDatabase();
            for (int i = 0; i < tags.Length; i++)
            {
                if (db.SetContains(FormatTag(tags[i]), key))
                {
                    return true;
                }
            }
            return false;
        }
        
        /// <inheritdoc />
        public bool IsHashFieldInTag<T>(string key, T field, params string[] tags)
        {
            if (tags == null || tags.Length == 0)
            {
                return false;
            }
            var db = RedisConnection.GetDatabase();
            for (int i = 0; i < tags.Length; i++)
            {
                if (db.SetContains(FormatTag(tags[i]), FormatSerializedMember(key, TagHashSeparator, field)))
                {
                    return true;
                }
            }
            return false;
        }

        /// <inheritdoc />
        public bool IsSetMemberInTag<T>(string key, T member, params string[] tags)
        {
            if (tags == null || tags.Length == 0)
            {
                return false;
            }
            var db = RedisConnection.GetDatabase();
            for (int i = 0; i < tags.Length; i++)
            {
                if (db.SetContains(FormatTag(tags[i]), FormatSerializedMember(key, TagSetSeparator, member)))
                {
                    return true;
                }
            }
            return false;
        }

        /// <inheritdoc />
        public async Task<bool> IsStringKeyInTagAsync(string key, params string[] tags)
        {
            if (tags == null || tags.Length == 0)
            {
                return false;
            }
            var db = RedisConnection.GetDatabase();
            for (int i = 0; i < tags.Length; i++)
            {
                if (await db.SetContainsAsync(FormatTag(tags[i]), key).ForAwait())
                {
                    return true;
                }
            }
            return false;
        }
        /// <inheritdoc />
        public async Task<bool> IsHashFieldInTagAsync<T>(string key, T field, params string[] tags)
        {
            if (tags == null || tags.Length == 0)
            {
                return false;
            }
            var db = RedisConnection.GetDatabase();
            for (int i = 0; i < tags.Length; i++)
            {
                if (await db.SetContainsAsync(FormatTag(tags[i]), FormatSerializedMember(key, TagHashSeparator, field)).ForAwait())
                {
                    return true;
                }
            }
            return false;
        }
        /// <inheritdoc />
        public async Task<bool> IsSetMemberInTagAsync<T>(string key, T member, params string[] tags)
        {
            if (tags == null || tags.Length == 0)
            {
                return false;
            }
            var db = RedisConnection.GetDatabase();
            for (int i = 0; i < tags.Length; i++)
            {
                if (await db.SetContainsAsync(FormatTag(tags[i]), FormatSerializedMember(key, TagSetSeparator, member)).ForAwait())
                {
                    return true;
                }
            }
            return false;
        }

        #endregion

        #region Private Methods
        /// <summary>
        /// Sets the maximum TTL between the current key TTL and the given TTL. Return the batch task to await
        /// </summary>
        /// <param name="batch">The batch context.</param>
        /// <param name="key">The key to compare and (eventually) set the expiration.</param>
        /// <param name="ttl">The TTL.</param>
        private static async Task<Task> SetMaxExpirationAsync(IBatch batch, string key, TimeSpan? ttl, CommandFlags flags = CommandFlags.None)
        {
            TimeSpan? final;
            IDatabase db = batch.Multiplexer.GetDatabase();
            bool preexistent = await db.KeyExistsAsync(key, flags).ForAwait();
            var currTtl = await db.KeyTimeToLiveAsync(key, flags).ForAwait();
            TimeSpan? curr = preexistent ? currTtl : null;
            if (curr != null)
            {
                // We have an expiration on both keys, use the max for the key
                final = curr > ttl ? curr : ttl;
            }
            else
            {
                final = ttl;
            }
            if (final == TimeSpan.MaxValue)
            {
                 return batch.KeyPersistAsync(key, flags); // not awaited here
            }
            else
            {
                return batch.KeyExpireAsync(key, final, flags); // not awaited here
            }
        }

        private static void SetMaxExpiration(IBatch batch, string key, TimeSpan? ttl, CommandFlags flags = CommandFlags.None)
        {
            if (ttl == null)
            {
                return;
            }
            TimeSpan? final;
            IDatabase db = batch.Multiplexer.GetDatabase();
            bool preexistent = db.KeyExists(key, flags);
            TimeSpan? curr = preexistent ? db.KeyTimeToLive(key, flags) : null;
            if (curr != null)
            {
                // We have an expiration on both keys, use the max for the key
                final = curr > ttl ? curr : ttl;
            }
            else
            {
                final = ttl;
            }
            if (final == TimeSpan.MaxValue)
            {
                batch.KeyPersistAsync(key, flags);
            }
            else
            {
                batch.KeyExpireAsync(key, final, flags);
            }
        }

        /// <summary>
        /// Get all the keys related to a tag(s), the keys returned are not tested for existence.
        /// </summary>
        /// <param name="db">The database.</param>
        /// <param name="tags">The tags.</param>
        private static ISet<RedisValue> GetTaggedItemsNoCleanup(IDatabase db, params string[] tags)
        {
            var keys = new List<RedisValue>();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                if (db.KeyType(tag) == RedisType.Set)
                {
                    keys.AddRange(db.SetMembers(tag));
                }
            }
            return new HashSet<RedisValue>(keys);
        }

        private async static Task<ISet<RedisValue>> GetTaggedItemsNoCleanupAsync(IDatabase db, params string[] tags)
        {
            var keys = new List<RedisValue>();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                var keyType = await db.KeyTypeAsync(tag).ForAwait();
                if (keyType == RedisType.Set)
                {
                    var setMembers = await db.SetMembersAsync(tag).ForAwait();
                    keys.AddRange(setMembers);
                }
            }
            return new HashSet<RedisValue>(keys);
        }

        /// <summary>
        /// Get all the keys related to a tag(s), only returns the keys that currently exists.
        /// </summary>
        /// <param name="db">The database.</param>
        /// <param name="tags">The tags.</param>
        private static ISet<RedisValue> GetTaggedItemsWithCleanup(IDatabase db, params string[] tags)
        {
            bool exists;
            var ret = new HashSet<RedisValue>();
            var toRemove = new List<RedisValue>();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                if (db.KeyType(tag) == RedisType.Set)
                {
                    var tagMembers = db.SetMembers(tag);
                    //Get the existing keys and delete the dead keys
                    foreach (var tagMember in tagMembers)
                    {
                        var tmString = tagMember.ToString();
                        if (tmString.Contains(TagHashSeparator))
                        {
                            // It's a hash field
                            var items = tmString.Split(new[] { TagHashSeparator }, 2, StringSplitOptions.None);
                            var hashKey = items[0];
                            var hashField = GetHashFieldItem(hashKey, tagMember);
                            exists = db.HashExists(hashKey, hashField);
                        }
                        else if (tmString.Contains(TagSetSeparator))
                        {
                            // It's a set/sorted set member
                            var items = tmString.Split(new[] { TagSetSeparator }, 2, StringSplitOptions.None);
                            var setKey = items[0];
                            var keyType = db.KeyType(setKey);
                            byte[] setMember = GetMemberSetItem(setKey, tagMember);
                            if (keyType == RedisType.SortedSet)
                            {
                                exists = db.SortedSetRank(setKey, setMember).HasValue;
                            }
                            else
                            {
                                exists = db.SetContains(setKey, setMember);
                            }
                        }
                        else
                        {
                            // It's a string
                            exists = db.KeyExists(tmString);
                        }
                        if (exists)
                        {
                            ret.Add(tagMember);
                        }
                        else
                        {
                            toRemove.Add(tagMember);
                        }
                    }
                    if (toRemove.Count > 0)
                    {
                        db.SetRemove(tag, toRemove.ToArray());
                        toRemove.Clear();
                    }
                }
            }
            return ret;
        }

        private async static Task<ISet<RedisValue>> GetTaggedItemsWithCleanupAsync(IDatabase db, params string[] tags)
        {
            bool exists;
            var ret = new HashSet<RedisValue>();
            var toRemove = new List<RedisValue>();
            foreach (var tagName in tags)
            {
                var tag = FormatTag(tagName);
                if (db.KeyType(tag) == RedisType.Set)
                {
                    var tagMembers = await db.SetMembersAsync(tag).ForAwait();
                    //Get the existing keys and delete the dead keys
                    foreach (var tagMember in tagMembers)
                    {
                        var tmString = tagMember.ToString();
                        if (tmString.Contains(TagHashSeparator))
                        {
                            // It's a hash
                            var items = tmString.Split(new[] { TagHashSeparator }, StringSplitOptions.None);
                            var hashKey = items[0];
                            var hashField = GetHashFieldItem(hashKey, tagMember);
                            exists = await db.HashExistsAsync(hashKey, hashField).ForAwait();
                        }
                        else if (tmString.Contains(TagSetSeparator))
                        {
                            // It's a set member
                            var items = tmString.Split(new[] { TagSetSeparator }, 2, StringSplitOptions.None);
                            var setKey = items[0];
                            var keyType = await db.KeyTypeAsync(setKey).ForAwait();
                            byte[] setMember = GetMemberSetItem(setKey, tagMember);
                            if (keyType == RedisType.SortedSet)
                            {
                                exists = (await db.SortedSetRankAsync(setKey, setMember).ForAwait()).HasValue;
                            }
                            else
                            {
                                exists = await db.SetContainsAsync(setKey, setMember).ForAwait();
                            }
                        }
                        else
                        {
                            // It's a string
                            exists = await db.KeyExistsAsync(tmString).ForAwait();
                        }
                        if (exists)
                        {
                            ret.Add(tagMember);
                        }
                        else
                        {
                            toRemove.Add(tagMember);
                        }
                    }
                    if (toRemove.Count > 0)
                    {
                        await db.SetRemoveAsync(tag, toRemove.ToArray()).ForAwait();
                        toRemove.Clear();
                    }
                }
            }
            return ret;
        }

        /// <summary>
        /// Gets the set member value part of a tag member.
        /// </summary>
        private static byte[] GetMemberSetItem(string setKey, RedisValue tagMember)
        {
            var arrTagMember = (byte[])tagMember;
            var prefixLen = System.Text.Encoding.UTF8.GetBytes(setKey + TagSetSeparator).Length;
            byte[] setMember = new byte[arrTagMember.Length - prefixLen];
            Array.Copy(arrTagMember, prefixLen, setMember, 0, setMember.Length);
            return setMember;
        }

        /// <summary>
        /// Gets the hash field part of a tag member.
        /// </summary>
        private static byte[] GetHashFieldItem(string hashKey, RedisValue tagMember)
        {
            var arrTagMember = (byte[])tagMember;
            var prefixLen = System.Text.Encoding.UTF8.GetBytes(hashKey + TagHashSeparator).Length;
            byte[] hashField = new byte[arrTagMember.Length - prefixLen];
            Array.Copy(arrTagMember, prefixLen, hashField, 0, hashField.Length);
            return hashField;
        }

        /// <summary>
        /// Return the RedisKey used for a tag
        /// </summary>
        /// <param name="tag">The tag name</param>
        /// <returns>RedisKey.</returns>
        private static RedisKey FormatTag(string tag)
        {
            return string.Format(TagFormat, tag);
        }
        /// <summary>
        /// Return the RedisValue to use for a tag that points to a hash field
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="field">The field.</param>
        /// <returns>RedisKey.</returns>
        private static RedisValue FormatHashField(string key, string field)
        {
            // set_key:$_->_$:hash_field
            return key + TagHashSeparator + field;
        }
        /// <summary>
        /// Return the RedisValue to use for a tag that points to a serialized member (hash field/member set)
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="separator">The tag separator to use.</param>
        /// <param name="member">The member (hash field/member set).</param>
        private RedisValue FormatSerializedMember<T>(string key, string separator, T member)
        {
            // set_key:$_-S>_$:serialized_member
            byte[] prefix = System.Text.Encoding.UTF8.GetBytes(key + separator);
            byte[] memberSerialized = Serializer.Serialize(member);
            byte[] result = new byte[prefix.Length + memberSerialized.Length];
            Array.Copy(prefix, 0, result, 0, prefix.Length);
            Array.Copy(memberSerialized, 0, result, prefix.Length, memberSerialized.Length);
            return result;
        }

        /// <summary>
        /// Runs a Server command in all the master servers.
        /// </summary>
        /// <param name="action">The action.</param>
        private void RunInAllMasters(Action<IServer> action)
        {
            var masters = GetMastersServers();
            foreach (var server in masters)
            {
                action(RedisConnection.GetServer(server));
            }
        }
        /// <summary>
        /// Runs a Server command (that returns an enumeration) in all the master servers.
        /// </summary>
        /// <param name="action">The action.</param>
        private IEnumerable<T> EnumerateInAllMasters<T>(Func<IServer, T> action)
        {
            var masters = GetMastersServers();
            foreach (var server in masters)
            {
                yield return action(RedisConnection.GetServer(server));
            }
        }
        /// <summary>
        /// Gets the masters servers endpoints.
        /// </summary>
        private List<EndPoint> GetMastersServers()
        {
            var masters = new List<EndPoint>();
            foreach (var ep in RedisConnection.GetEndPoints())
            {
                var server = RedisConnection.GetServer(ep);
                if (server.IsConnected)
                {
                    if (server.ServerType == ServerType.Cluster)
                    {
                        masters.AddRange(server.ClusterConfiguration.Nodes.Where(n => !n.IsReplica).Select(n => n.EndPoint));
                        break;
                    }
                    if (server.ServerType == ServerType.Standalone && !server.IsReplica)
                    {
                        masters.Add(ep);
                        break;
                    }
                }
            }
            return masters;
        }

        #endregion
    }
}
