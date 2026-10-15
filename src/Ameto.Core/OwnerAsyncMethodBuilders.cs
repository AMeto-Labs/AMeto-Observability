using System.Runtime.CompilerServices;

namespace Ameto.Core;

/// <summary>
/// The async method builders of the methods that hold an owner — a flush slot, the swap lock, a
/// heavy-phase count, a batch's lease — across an await: <see cref="AsyncTaskMethodBuilder"/> and
/// <see cref="AsyncValueTaskMethodBuilder{TResult}"/>, with one test seam.
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
/// <para>In production they are the runtime's builders: the seam is read once, at a method's
/// first suspension, and is null.</para>
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
        Suspending<TStateMachine>();
        _inner.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        Suspending<TStateMachine>();
        _inner.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    private void Suspending<TStateMachine>()
    {
        if (_boxed) return;
        OwnerAsync.FirstSuspension(typeof(TStateMachine));
        _boxed = true;   // before the box copies the state machine, and this builder with it
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
        Suspending<TStateMachine>();
        _inner.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        Suspending<TStateMachine>();
        _inner.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    private void Suspending<TStateMachine>()
    {
        if (_boxed) return;
        OwnerAsync.FirstSuspension(typeof(TStateMachine));
        _boxed = true;
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
        Suspending<TStateMachine>();
        _inner.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        Suspending<TStateMachine>();
        _inner.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    private void Suspending<TStateMachine>()
    {
        if (_boxed) return;
        OwnerAsync.FirstSuspension(typeof(TStateMachine));
        _boxed = true;
    }
}
