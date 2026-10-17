using System.Runtime.CompilerServices;

namespace Ameto.Core;

/// <summary>
/// The async method builders of the methods that hold an owner — a flush slot, the swap lock, a
/// heavy-phase count, a batch's lease — across an await: <see cref="AsyncTaskMethodBuilder"/> and
/// <see cref="AsyncValueTaskMethodBuilder{TResult}"/>, with test seams.
///
/// <para>Why a seam is needed at all. An async method's state machine is boxed at its first real
/// suspension, inside <c>AwaitUnsafeOnCompleted</c> — an allocation, and one that runs out of memory
/// like any other. The compiler has recorded the suspension in the state by then, and it guards
/// every <c>finally</c> of the method with that state: a box that cannot be allocated throws into
/// the method with each finally skipped — its own, the ones it unwinds through, and every later
/// one until a suspension resumes — and a catch around the await does run. No ordinary seam can
/// stand in for that (a throw from before the await leaves the finallys running), so the methods
/// that own something across an await are built with these builders, which make the first
/// suspension throw exactly there when a test asks.</para>
///
/// <para>And not only the first (round-6 review, R6-H3's class). Every LATER suspension registers its
/// continuation on what it awaits, and that can throw too, at the same point and with the same effect:
/// the TPL event source and async debugging wrap the continuation (an allocation), a second continuation
/// on one task makes a list, and a task that completed meanwhile is queued to a thread-pool queue that
/// may have to grow. For a <see cref="Task"/> awaiter that throw reaches the method on a single out of
/// memory; for a <see cref="ValueTask"/>, a yield or any other awaiter the runtime catches it and rethrows
/// it on the pool (the process ends), unless that rethrow runs out of memory too. Either way every finally
/// is skipped from that await until a suspension resumes — including, when a catch takes the throw and the
/// method goes on, the finallys it leaves after the catch — while the catches run.
/// <see cref="OwnerAsync.LaterSuspensionFailsForTest"/> makes a later suspension throw there.</para>
///
/// <para>In production they are the runtime's builders: a suspension reads one field, null in production.</para>
/// </summary>
internal static class OwnerAsync
{
    /// <summary>
    /// Test seam: asked at an owner-holding async method's FIRST real suspension, with its state
    /// machine's type (whose name carries the method's); true makes that suspension throw
    /// <see cref="OutOfMemoryException"/> where the runtime allocates the box. An
    /// <see cref="AsyncLocal{T}"/>, so it reaches only the flows of the test that sets it — created by
    /// the first test that asks for it; production never does.
    ///
    /// <para>No static initializer (round-5 review M-A). The AsyncLocal was created by this type's
    /// initializer, which runs at the process's first owner-method suspension — and an out of memory
    /// there failed the type for good: every later first suspension threw TypeInitializationException,
    /// no swap, spill or despill ran again, and log ingest answered 503 or 500 until a restart. The
    /// field below is null in production, and reading it is all a suspension does.</para>
    /// </summary>
    internal static AsyncLocal<Func<Type, bool>?> FirstSuspensionFailsForTest =>
        Volatile.Read(ref s_firstSuspensionFails)
        ?? Interlocked.CompareExchange(ref s_firstSuspensionFails, new AsyncLocal<Func<Type, bool>?>(), null)
        ?? s_firstSuspensionFails!;

    /// <summary>What <see cref="FirstSuspensionFailsForTest"/> made; null until a test asks for it.</summary>
    private static AsyncLocal<Func<Type, bool>?>? s_firstSuspensionFails;

    /// <summary>Throws where the box would be allocated, when the seam says so.</summary>
    internal static void FirstSuspension(Type stateMachine)
    {
        if (Volatile.Read(ref s_firstSuspensionFails)?.Value is { } fails && fails(stateMachine))
            throw new OutOfMemoryException("test: the async state machine's box could not be allocated");
    }

    /// <summary>
    /// Test seam (round-5 review M-C): asked at an owner-holding async method's FIRST real suspension,
    /// with its state machine's type; true makes the box — the state machine copied into it, as the
    /// runtime makes it before it registers the continuation — and then throws, as a registration that
    /// fails after the box would (the TPL event source, async debugging, a second continuation, a queue's
    /// growth). In a Release build the method then goes on in its own copy of the state machine, while
    /// the builder keeps the box with the copy taken at that suspension. Created by the first test that
    /// asks for it, like <see cref="FirstSuspensionFailsForTest"/>.
    /// </summary>
    internal static AsyncLocal<Func<Type, bool>?> BoxThenFailForTest =>
        Volatile.Read(ref s_boxThenFail)
        ?? Interlocked.CompareExchange(ref s_boxThenFail, new AsyncLocal<Func<Type, bool>?>(), null)
        ?? s_boxThenFail!;

    /// <summary>What <see cref="BoxThenFailForTest"/> made; null until a test asks for it.</summary>
    private static AsyncLocal<Func<Type, bool>?>? s_boxThenFail;

    /// <summary>
    /// Test seam (round-6 review, R6-H3's class): asked at every LATER real suspension of an owner-holding
    /// async method — each one after the first, when its box already exists and the method runs in it — with
    /// its state machine's type and the awaiter's; true makes that suspension throw
    /// <see cref="OutOfMemoryException"/> after the compiler has recorded it in the state and before anything
    /// is registered, as a registration that fails does (see the type's remarks). What it awaited goes on. A
    /// test that wants the Nth such suspension counts in the function. Created by the first test that asks for
    /// it, like <see cref="FirstSuspensionFailsForTest"/>; null in production.
    /// </summary>
    internal static AsyncLocal<Func<Type, Type, bool>?> LaterSuspensionFailsForTest =>
        Volatile.Read(ref s_laterSuspensionFails)
        ?? Interlocked.CompareExchange(ref s_laterSuspensionFails, new AsyncLocal<Func<Type, Type, bool>?>(), null)
        ?? s_laterSuspensionFails!;

    /// <summary>What <see cref="LaterSuspensionFailsForTest"/> made; null until a test asks for it.</summary>
    private static AsyncLocal<Func<Type, Type, bool>?>? s_laterSuspensionFails;

    /// <summary>Throws where a later suspension's continuation would be registered, when the seam says so.</summary>
    internal static void LaterSuspension(Type stateMachine, Type awaiter)
    {
        if (Volatile.Read(ref s_laterSuspensionFails)?.Value is { } fails && fails(stateMachine, awaiter))
            throw new OutOfMemoryException("test: the registration of a later suspension's continuation failed");
    }

    /// <summary>Whether the seam asks this first suspension to make its box and then fail.</summary>
    internal static bool BoxThenFail(Type stateMachine) =>
        Volatile.Read(ref s_boxThenFail)?.Value is { } boxThenFail && boxThenFail(stateMachine);

    /// <summary>
    /// Makes the box <see cref="BoxThenFailForTest"/> just made hold what a Release build's holds: a COPY of
    /// the state machine as it was at this suspension (round-6 review, the LOW on M-C's test). In Release the
    /// state machine is a struct, copied into the box, and the method goes on in its own copy; a later await
    /// that reuses the box resumes the stale one. In Debug it is a class, the box holds the object itself,
    /// and the same later await resumed where the method was — so a test of that road could not fail in
    /// Debug, which is all CI runs. Here a class state machine is cloned member by member, after the box was
    /// made (the clone's builder points at the box, as a Release copy's does), and the clone put in the box.
    /// <paramref name="box"/> is what the builder made (its task); a box with no <c>StateMachine</c> field
    /// is left as it is. Test-only: reached only when the seam fires.
    /// </summary>
    internal static void LeaveACopyInTheBox(object? box)
    {
        if (box is null) return;
        const System.Reflection.BindingFlags Instance = System.Reflection.BindingFlags.Instance
                                                      | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic;
        var field = box.GetType().GetField("StateMachine", Instance);
        if (field?.GetValue(box) is not { } stateMachine || stateMachine.GetType().IsValueType) return;
        var clone = typeof(object).GetMethod("MemberwiseClone", Instance)!.Invoke(stateMachine, null);
        field.SetValue(box, clone);
    }

    /// <summary>The box behind a <see cref="ValueTask{TResult}"/> an owner builder handed out (its <c>_obj</c>), for <see cref="LeaveACopyInTheBox"/>.</summary>
    internal static object? BoxOf<TResult>(ValueTask<TResult> task) =>
        typeof(ValueTask<TResult>).GetField("_obj", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)?.GetValue(task);

    /// <summary>A task that never completes: the box <see cref="BoxThenFailForTest"/> makes is registered on it.</summary>
    internal static Task Never => (s_never ??= new TaskCompletionSource()).Task;
    private static TaskCompletionSource? s_never;
}

/// <summary><see cref="AsyncTaskMethodBuilder"/> with <see cref="OwnerAsync"/>'s seam.</summary>
internal struct OwnerTaskMethodBuilder
{
    private AsyncTaskMethodBuilder _inner;
    private bool                   _boxed;

    public static OwnerTaskMethodBuilder Create() => new() { _inner = AsyncTaskMethodBuilder.Create() };

    public Task Task => _inner.Task;

    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine =>
        _inner.Start(ref stateMachine);

    public void SetStateMachine(IAsyncStateMachine stateMachine) => _inner.SetStateMachine(stateMachine);

    public void SetResult() => _inner.SetResult();

    public void SetException(Exception exception) => _inner.SetException(exception);

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (_boxed) OwnerAsync.LaterSuspension(typeof(TStateMachine), typeof(TAwaiter));
        else if (Suspending<TStateMachine>()) BoxThenFail(ref stateMachine);
        _inner.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (_boxed) OwnerAsync.LaterSuspension(typeof(TStateMachine), typeof(TAwaiter));
        else if (Suspending<TStateMachine>()) BoxThenFail(ref stateMachine);
        _inner.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    /// <summary>The seams, at the first suspension: true when <see cref="OwnerAsync.BoxThenFailForTest"/> asks for the box and then a failure.</summary>
    private bool Suspending<TStateMachine>()
    {
        if (_boxed) return false;
        OwnerAsync.FirstSuspension(typeof(TStateMachine));
        _boxed = true;   // before the box copies the state machine, and this builder with it
        return OwnerAsync.BoxThenFail(typeof(TStateMachine));
    }

    /// <summary>
    /// Makes the box — registered on a task that never completes — and throws as a registration that failed
    /// after it. The box holds a copy of the state machine in Debug too (<see cref="OwnerAsync.LeaveACopyInTheBox"/>).
    /// </summary>
    private void BoxThenFail<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        var never = OwnerAsync.Never.ConfigureAwait(false).GetAwaiter();
        _inner.AwaitUnsafeOnCompleted(ref never, ref stateMachine);
        OwnerAsync.LeaveACopyInTheBox(_inner.Task);
        throw new OutOfMemoryException("test: the box was made, then the registration failed");
    }
}

/// <summary>
/// <see cref="AsyncTaskMethodBuilder{TResult}"/> with <see cref="OwnerAsync"/>'s seam: for a method that
/// is handed something to read by a caller who owns it — a flush reading a frozen tier — whose caller
/// takes the method's fault for "it has stopped reading" (round-5 H-B).
/// </summary>
internal struct OwnerTaskMethodBuilder<TResult>
{
    private AsyncTaskMethodBuilder<TResult> _inner;
    private bool                            _boxed;

    public static OwnerTaskMethodBuilder<TResult> Create() => new() { _inner = AsyncTaskMethodBuilder<TResult>.Create() };

    public Task<TResult> Task => _inner.Task;

    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine =>
        _inner.Start(ref stateMachine);

    public void SetStateMachine(IAsyncStateMachine stateMachine) => _inner.SetStateMachine(stateMachine);

    public void SetResult(TResult result) => _inner.SetResult(result);

    public void SetException(Exception exception) => _inner.SetException(exception);

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (_boxed) OwnerAsync.LaterSuspension(typeof(TStateMachine), typeof(TAwaiter));
        else if (Suspending<TStateMachine>()) BoxThenFail(ref stateMachine);
        _inner.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (_boxed) OwnerAsync.LaterSuspension(typeof(TStateMachine), typeof(TAwaiter));
        else if (Suspending<TStateMachine>()) BoxThenFail(ref stateMachine);
        _inner.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    /// <summary>See <see cref="OwnerTaskMethodBuilder"/>.</summary>
    private bool Suspending<TStateMachine>()
    {
        if (_boxed) return false;
        OwnerAsync.FirstSuspension(typeof(TStateMachine));
        _boxed = true;
        return OwnerAsync.BoxThenFail(typeof(TStateMachine));
    }

    /// <summary>See <see cref="OwnerTaskMethodBuilder"/>.</summary>
    private void BoxThenFail<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        var never = OwnerAsync.Never.ConfigureAwait(false).GetAwaiter();
        _inner.AwaitUnsafeOnCompleted(ref never, ref stateMachine);
        OwnerAsync.LeaveACopyInTheBox(_inner.Task);
        throw new OutOfMemoryException("test: the box was made, then the registration failed");
    }
}

/// <summary><see cref="AsyncValueTaskMethodBuilder{TResult}"/> with <see cref="OwnerAsync"/>'s seam.</summary>
internal struct OwnerValueTaskMethodBuilder<TResult>
{
    private AsyncValueTaskMethodBuilder<TResult> _inner;
    private bool                                 _boxed;

    public static OwnerValueTaskMethodBuilder<TResult> Create() =>
        new() { _inner = AsyncValueTaskMethodBuilder<TResult>.Create() };

    public ValueTask<TResult> Task => _inner.Task;

    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine =>
        _inner.Start(ref stateMachine);

    public void SetStateMachine(IAsyncStateMachine stateMachine) => _inner.SetStateMachine(stateMachine);

    public void SetResult(TResult result) => _inner.SetResult(result);

    public void SetException(Exception exception) => _inner.SetException(exception);

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (_boxed) OwnerAsync.LaterSuspension(typeof(TStateMachine), typeof(TAwaiter));
        else if (Suspending<TStateMachine>()) BoxThenFail(ref stateMachine);
        _inner.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (_boxed) OwnerAsync.LaterSuspension(typeof(TStateMachine), typeof(TAwaiter));
        else if (Suspending<TStateMachine>()) BoxThenFail(ref stateMachine);
        _inner.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    /// <summary>See <see cref="OwnerTaskMethodBuilder"/>.</summary>
    private bool Suspending<TStateMachine>()
    {
        if (_boxed) return false;
        OwnerAsync.FirstSuspension(typeof(TStateMachine));
        _boxed = true;
        return OwnerAsync.BoxThenFail(typeof(TStateMachine));
    }

    /// <summary>See <see cref="OwnerTaskMethodBuilder"/>.</summary>
    private void BoxThenFail<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        var never = OwnerAsync.Never.ConfigureAwait(false).GetAwaiter();
        _inner.AwaitUnsafeOnCompleted(ref never, ref stateMachine);
        OwnerAsync.LeaveACopyInTheBox(OwnerAsync.BoxOf(_inner.Task));
        throw new OutOfMemoryException("test: the box was made, then the registration failed");
    }
}
