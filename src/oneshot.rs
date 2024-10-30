// SPDX-License-Identifier: LGPL-3.0-or-later OR MPL-2.0
// This file is a part of `unsend`.
//
// `unsend` is free software: you can redistribute it and/or modify it under the
// terms of either:
//
// * GNU Lesser General Public License as published by the Free Software Foundation, either
//   version 3 of the License, or (at your option) any later version.
// * Mozilla Public License as published by the Mozilla Foundation, version 2.
//
// `unsend` is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE. See the GNU Lesser General Public License or the Mozilla Public License for more
// details.
//
// You should have received a copy of the GNU Lesser General Public License and the Mozilla
// Public License along with `unsend`. If not, see <https://www.gnu.org/licenses/>.

use crate::channel::{ChannelClosed, TryRecvError};

use alloc::rc::Rc;

use core::cell::Cell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

struct Channel<T>(Cell<State<T>>);

enum State<T> {
    /// The channel is open and waiting for a value.
    Open,

    /// Either end of the channel has been closed.
    Closed,

    /// The receiving end of the channel is waiting for a value.
    Waiting(Waker),

    /// The sender has sent a value and is waiting for the receiver to read it.
    Value(T),
}

/// A sender for an MPMC channel.
pub struct Sender<T> {
    /// The origin channel.
    channel: Rc<Channel<T>>,
}

/// A receiver for an MPMC channel.
pub struct Receiver<T> {
    /// The origin channel.
    channel: Rc<Channel<T>>,
}

/// Create a new oneshot channel.
///
/// The only advantage over the MPMC channel is that it saves an allocation.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel = Rc::new(Channel(Cell::new(State::Open)));

    (
        Sender {
            channel: channel.clone(),
        },
        Receiver { channel },
    )
}

impl<T> Sender<T> {
    /// Send an item.
    pub fn send(self, item: T) -> Result<(), ChannelClosed> {
        match self.channel.0.replace(State::Value(item)) {
            State::Open => Ok(()),

            State::Closed => Err(ChannelClosed::new()),

            State::Waiting(waker) => {
                waker.wake();
                Ok(())
            }

            State::Value(_) => panic!("cannot send twice on a oneshot channel"),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        match self.channel.0.replace(State::Closed) {
            State::Value(value) => {
                // Don't let the value out.
                self.channel.0.set(State::Value(value));
            }

            State::Waiting(waker) => waker.wake(),

            _ => {}
        }
    }
}

impl<T> Receiver<T> {
    /// Try to receive an item.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.channel.0.replace(State::Closed) {
            State::Value(value) => Ok(value),

            State::Open => {
                self.channel.0.set(State::Open);
                Err(TryRecvError::Empty)
            }

            State::Closed => Err(TryRecvError::Closed),

            State::Waiting(waker) => {
                self.channel.0.set(State::Waiting(waker));
                Err(TryRecvError::Empty)
            }
        }
    }

    /// Receive an item.
    pub async fn recv(self) -> Result<T, ChannelClosed> {
        PollFn(
            move |cx: &mut Context<'_>| match self.channel.0.replace(State::Closed) {
                State::Value(value) => Poll::Ready(Ok(value)),

                State::Open => {
                    self.channel.0.set(State::Waiting(cx.waker().clone()));
                    Poll::Pending
                }

                State::Closed => Poll::Ready(Err(ChannelClosed::new())),

                State::Waiting(mut waker) => {
                    if !cx.waker().will_wake(&waker) {
                        waker = cx.waker().clone();
                    }
                    self.channel.0.set(State::Waiting(waker));
                    Poll::Pending
                }
            },
        )
        .await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let State::Waiting(waker) = self.channel.0.replace(State::Closed) {
            waker.wake();
        }
    }
}

struct PollFn<F>(F);

impl<F> Unpin for PollFn<F> {}

impl<T, F: FnMut(&mut Context<'_>) -> Poll<T>> Future for PollFn<F> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        (self.0)(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future;

    #[test]
    fn test_recv() {
        future::block_on(async {
            let (sender, receiver) = channel();

            sender.send(1).unwrap();
            assert_eq!(receiver.recv().await.unwrap(), 1);
        });
    }

    #[test]
    fn test_try_recv() {
        future::block_on(async {
            let (sender, receiver) = channel();

            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
            sender.send(1).unwrap();
            assert_eq!(receiver.try_recv().unwrap(), 1);
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Closed)));
        });
    }

    #[test]
    fn test_recv_dropped() {
        future::block_on(async {
            let (sender, receiver) = channel();

            drop(receiver);
            assert!(sender.send(1).is_err());
        });
    }

    #[test]
    fn test_send_dropped() {
        future::block_on(async {
            let (sender, receiver) = channel::<()>();

            drop(sender);
            assert!(receiver.recv().await.is_err());
        });
    }
}
