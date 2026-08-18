#include "SlippiSpectate.h"
#include "Common/CommonTypes.h"
#include "Common/Logging/Log.h"
#include "base64.hpp"
#include <Core/ConfigManager.h>

// Networking
#ifdef _WIN32
#include <share.h>
#include <ws2tcpip.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#endif

// CALLED FROM DOLPHIN MAIN THREAD
SlippiSpectateServer *SlippiSpectateServer::getInstance()
{
	static SlippiSpectateServer instance; // Guaranteed to be destroyed.
	                                      // Instantiated on first use.
	return &instance;
}

// CALLED FROM DOLPHIN MAIN THREAD
void SlippiSpectateServer::write(u8 *payload, u32 length)
{
	if (!SConfig::GetInstance().m_enableSpectator)
	{
		return;
	}
	// Keep the channel's receive side drained even while nothing reads
	// pads (menus): input batches sent every step must never back up
	// the socket. Runs on every event write, i.e. every frame.
	directDrainInputs(false);
	directWrite(payload, length);
	std::string str_payload((char *)payload, length);
	m_event_queue.Push(str_payload);
	m_wake_cv.notify_one();
}

// CALLED FROM DOLPHIN MAIN THREAD. See header.
bool SlippiSpectateServer::directDrainInputs(bool block)
{
#ifndef _WIN32
	if (block)
	{
		// A blocking call is the lockstep gate: only a batch that
		// arrives from here on counts as this frame's inputs.
		m_direct_got_batch = false;
	}

	while (1)
	{
		int fd = m_direct_fd.load(std::memory_order_acquire);
		if (fd < 0)
		{
			return m_direct_got_batch;
		}

		u8 buf[4096];
		ssize_t got = recv(fd, buf, sizeof(buf), 0);
		if (got > 0)
		{
			m_direct_rx.insert(m_direct_rx.end(), buf, buf + got);

			// Parse complete <u32 len><payload> frames.
			while (m_direct_rx.size() >= 4)
			{
				u32 len = ((u32)m_direct_rx[0] << 24) | ((u32)m_direct_rx[1] << 16) |
				          ((u32)m_direct_rx[2] << 8) | (u32)m_direct_rx[3];
				if (len > 4096)
				{
					WARN_LOG(SLIPPI, "Direct channel input frame too large (%u); dropping client", len);
					m_direct_fd.store(-1, std::memory_order_release);
					close(fd);
					m_direct_rx.clear();
					return m_direct_got_batch;
				}
				if (m_direct_rx.size() < 4 + len)
				{
					break;
				}

				const u8 *payload = m_direct_rx.data() + 4;
				// 0x01 = pad batch: count, then per pad: port (1-4) + 8 bytes.
				if (len >= 2 && payload[0] == 0x01)
				{
					u8 count = payload[1];
					if (len == (u32)(2 + count * 9))
					{
						for (u8 i = 0; i < count; i++)
						{
							const u8 *entry = payload + 2 + i * 9;
							u8 port = entry[0];
							if (port >= 1 && port <= 4)
							{
								memcpy(m_direct_pad_bufs[port - 1], entry + 1, 8);
								m_direct_pad_set[port - 1] = true;
							}
						}
						m_direct_got_batch = true;
					}
				}

				m_direct_rx.erase(m_direct_rx.begin(), m_direct_rx.begin() + 4 + len);
			}

			continue; // there may be more readable data
		}

		if (got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
		{
			// Client hung up (or hard error): drop it so blocked callers
			// fall through instead of waiting forever.
			m_direct_fd.store(-1, std::memory_order_release);
			close(fd);
			m_direct_rx.clear();
			return m_direct_got_batch;
		}

		// Nothing readable right now.
		if (!block || m_direct_got_batch)
		{
			return m_direct_got_batch;
		}

		struct pollfd pfd = {fd, POLLIN, 0};
		poll(&pfd, 1, 10);

		if (m_stop_socket_thread)
		{
			return m_direct_got_batch;
		}
	}
#else
	(void)block;
	return false;
#endif
}

// CALLED FROM DOLPHIN MAIN THREAD. See header.
bool SlippiSpectateServer::directPad(int port, u8 *out)
{
#ifndef _WIN32
	if (port < 1 || port > 4 || !m_direct_pad_set[port - 1])
	{
		return false;
	}
	memcpy(out, m_direct_pad_bufs[port - 1], 8);
	return true;
#else
	(void)port;
	(void)out;
	return false;
#endif
}

// CALLED FROM DOLPHIN MAIN THREAD. Sends one length-prefixed payload to
// the direct-channel client, if any. Non-blocking: rather than ever
// stalling emulation on a slow client, the client is dropped (it will
// notice the closed socket and can reconnect).
void SlippiSpectateServer::directWrite(const u8 *payload, u32 length)
{
#ifndef _WIN32
	int fd = m_direct_fd.load(std::memory_order_acquire);
	if (fd < 0)
	{
		return;
	}

	u8 header[4] = {(u8)(length >> 24), (u8)(length >> 16), (u8)(length >> 8), (u8)length};
	struct iovec iov[2];
	iov[0].iov_base = header;
	iov[0].iov_len = 4;
	iov[1].iov_base = (void *)payload;
	iov[1].iov_len = length;

	struct msghdr msg = {};
	msg.msg_iov = iov;
	msg.msg_iovlen = 2;

	ssize_t sent = sendmsg(fd, &msg, MSG_NOSIGNAL);
	if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
	{
		WARN_LOG(SLIPPI, "Direct channel client too slow; dropping it");
	}
	else if (sent == (ssize_t)(4 + length))
	{
		return;
	}
	else
	{
		// Partial writes would desync the framing; treat them (and any
		// other error) like a disconnect too. The send buffer is large
		// enough (see directAccept) that a healthy client never hits
		// this.
		WARN_LOG(SLIPPI, "Direct channel write failed (%zd); dropping client", sent);
	}

	m_direct_fd.store(-1, std::memory_order_release);
	close(fd);
#else
	(void)payload;
	(void)length;
#endif
}

// CALLED FROM SERVER THREAD
void SlippiSpectateServer::directListen(const std::string &path)
{
#ifndef _WIN32
	unlink(path.c_str());

	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if (fd < 0)
	{
		WARN_LOG(SLIPPI, "Could not create direct channel socket at %s", path.c_str());
		return;
	}

	struct sockaddr_un addr = {};
	addr.sun_family = AF_UNIX;
	if (path.length() >= sizeof(addr.sun_path))
	{
		WARN_LOG(SLIPPI, "Direct channel path too long: %s", path.c_str());
		close(fd);
		return;
	}
	strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

	if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(fd, 1) < 0)
	{
		WARN_LOG(SLIPPI, "Could not listen on direct channel socket at %s", path.c_str());
		close(fd);
		return;
	}

	m_direct_listen_fd = fd;
	INFO_LOG(SLIPPI, "Direct channel listening at %s", path.c_str());
#else
	(void)path;
#endif
}

// CALLED FROM SERVER THREAD, each loop iteration. Non-blocking accept;
// a new client replaces any previous one.
void SlippiSpectateServer::directAccept()
{
#ifndef _WIN32
	if (m_direct_listen_fd < 0)
	{
		return;
	}

	int client = accept(m_direct_listen_fd, nullptr, nullptr);
	if (client < 0)
	{
		return;
	}

	// The game thread must never block on this socket: non-blocking,
	// with a send buffer deep enough (~2000 frames at ~500B) that only
	// a genuinely dead or wedged client ever fills it.
	int flags = fcntl(client, F_GETFL, 0);
	fcntl(client, F_SETFL, flags | O_NONBLOCK);
	int sndbuf = 1 << 20;
	setsockopt(client, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

	int old = m_direct_fd.exchange(client, std::memory_order_acq_rel);
	if (old >= 0)
	{
		close(old);
	}

	INFO_LOG(SLIPPI, "Direct channel client connected");
#endif
}

// CALLED FROM DOLPHIN MAIN THREAD
void SlippiSpectateServer::startGame()
{
	if (!SConfig::GetInstance().m_enableSpectator)
	{
		return;
	}

	json start_game_message;
	start_game_message["type"] = "start_game";
	m_event_queue.Push(start_game_message.dump());
	m_wake_cv.notify_one();
}

// CALLED FROM DOLPHIN MAIN THREAD
void SlippiSpectateServer::endGame(bool dolphin_closed)
{
	if (!SConfig::GetInstance().m_enableSpectator)
	{
		return;
	}
	json end_game_message;
	end_game_message["type"] = "end_game";
	end_game_message["dolphin_closed"] = dolphin_closed;
	m_event_queue.Push(end_game_message.dump());
	m_wake_cv.notify_one();
}

// CALLED FROM SERVER THREAD
void SlippiSpectateServer::writeEvents(u16 peer_id)
{
	// Send menu events
	if (!m_in_game && (m_sockets[peer_id]->m_menu_cursor != m_menu_cursor))
	{
		ENetPacket *packet = enet_packet_create(m_menu_event.data(), m_menu_event.length(), ENET_PACKET_FLAG_RELIABLE);
		// Batch for sending
		enet_peer_send(m_sockets[peer_id]->m_peer, 0, packet);
		// Record for the peer that it was sent
		m_sockets[peer_id]->m_menu_cursor = m_menu_cursor;
	}

	// Send game events
	// Loop through each event that needs to be sent
	//  send all the events starting at their cursor
	// If the client's cursor is beyond the end of the event buffer, then
	//  it's probably left over from an old game. (Or is invalid anyway)
	//  So reset it back to 0
	if (m_sockets[peer_id]->m_cursor > m_event_buffer.size())
	{
		m_sockets[peer_id]->m_cursor = 0;
	}

	for (u64 i = m_sockets[peer_id]->m_cursor; i < m_event_buffer.size(); i++)
	{
		ENetPacket *packet =
		    enet_packet_create(m_event_buffer[i].data(), m_event_buffer[i].size(), ENET_PACKET_FLAG_RELIABLE);
		// Batch for sending
		enet_peer_send(m_sockets[peer_id]->m_peer, 0, packet);
		m_sockets[peer_id]->m_cursor++;
	}
}

// CALLED FROM SERVER THREAD
void SlippiSpectateServer::popEvents()
{
	// Loop through the event queue and keep popping off events and handling them
	while (!m_event_queue.Empty())
	{
		std::string event;
		m_event_queue.Pop(event);
		// These two are meta-events, used to signify the start/end of a game
		json json_message = json::parse(event, nullptr, false);
		if (!json_message.is_discarded() && (json_message.find("type") != json_message.end()))
		{
			if (json_message["type"] == "end_game")
			{
				u32 cursor = (u32)(m_event_buffer.size() + m_cursor_offset);
				json_message["cursor"] = cursor;
				json_message["next_cursor"] = cursor + 1;
				m_menu_cursor = 0;
				m_event_buffer.push_back(json_message.dump());
				m_cursor_offset += m_event_buffer.size();
				m_menu_event.clear();
				m_in_game = false;
				continue;
			}
			if (json_message["type"] == "start_game")
			{
				m_event_buffer.clear();
				u32 cursor = (u32)(m_event_buffer.size() + m_cursor_offset);
				m_in_game = true;
				json_message["cursor"] = cursor;
				json_message["next_cursor"] = cursor + 1;
				m_event_buffer.push_back(json_message.dump());
				continue;
			}
		}

		// Make json wrapper for game event
		json game_event;

		if (!m_in_game)
		{
			game_event["payload"] = base64::Base64::Encode(event);
			m_menu_cursor += 1;
			game_event["type"] = "menu_event";
			m_menu_event = game_event.dump();
			continue;
		}

		u8 command = (u8)event[0];
		m_event_concat = m_event_concat + event;

		static std::unordered_map<u8, bool> sendEvents = {
		    {0x36, true}, // GAME_INIT
		    {0x3C, true}, // FRAME_END
		    {0x39, true}, // GAME_END
		    {0x10, true}, // SPLIT_MESSAGE
		};

		if (sendEvents.count(command))
		{
			u32 cursor = (u32)(m_event_buffer.size() + m_cursor_offset);
			game_event["payload"] = base64::Base64::Encode(m_event_concat);
			game_event["type"] = "game_event";
			game_event["cursor"] = cursor;
			game_event["next_cursor"] = cursor + 1;
			m_event_buffer.push_back(game_event.dump());

			m_event_concat = "";
		}
	}
}

// CALLED ONCE EVER, DOLPHIN MAIN THREAD
SlippiSpectateServer::SlippiSpectateServer()
{
	if (!SConfig::GetInstance().m_enableSpectator)
	{
		return;
	}

	m_in_game = false;
	m_menu_cursor = 0;
	m_menu_event.clear();
	m_cursor_offset = 0;

	// Spawn thread for socket listener
	m_stop_socket_thread = false;
	m_socketThread = std::thread(&SlippiSpectateServer::SlippicommSocketThread, this);
}

// CALLED FROM DOLPHIN MAIN THREAD
SlippiSpectateServer::~SlippiSpectateServer()
{
	// The socket thread will be blocked waiting for input
	// So to wake it up, let's connect to the socket!
	m_stop_socket_thread = true;
	m_wake_cv.notify_one();
	if (m_socketThread.joinable())
	{
		m_socketThread.join();
	}
}

// CALLED FROM SERVER THREAD
void SlippiSpectateServer::handleMessage(u8 *buffer, u32 length, u16 peer_id)
{
	// Unpack the message
	std::string message((char *)buffer, length);
	json json_message = json::parse(message, nullptr, false);
	if (!json_message.is_discarded() && (json_message.find("type") != json_message.end()))
	{
		// Check what type of message this is
		if (!json_message["type"].is_string())
		{
			return;
		}

		if (json_message["type"] == "connect_request")
		{
			// Get the requested cursor
			if (json_message.find("cursor") == json_message.end())
			{
				return;
			}
			if (!json_message["cursor"].is_number_integer())
			{
				return;
			}
			u32 requested_cursor = json_message["cursor"];
			u32 sent_cursor = 0;
			// Set the user's cursor position
			if (requested_cursor >= m_cursor_offset)
			{
				// If the requested cursor is past what events we even have, then just tell them to start over
				if (requested_cursor > m_event_buffer.size() + m_cursor_offset)
				{
					m_sockets[peer_id]->m_cursor = 0;
				}
				// Requested cursor is in the middle of a live match, events that we have
				else
				{
					m_sockets[peer_id]->m_cursor = requested_cursor - m_cursor_offset;
				}
			}
			else
			{
				// The client requested a cursor that was too low. Bring them up to the present
				m_sockets[peer_id]->m_cursor = 0;
			}

			sent_cursor = (u32)m_sockets[peer_id]->m_cursor + (u32)m_cursor_offset;

			// If someone joins while at the menu, don't catch them up
			//  set their cursor to the end
			if (!m_in_game)
			{
				m_sockets[peer_id]->m_cursor = m_event_buffer.size();
			}

			json reply;
			reply["type"] = "connect_reply";
			reply["nick"] = "Slippi Online";
			reply["version"] = scm_slippi_semver_str;
			reply["cursor"] = sent_cursor;

			std::string packet_buffer = reply.dump();

			ENetPacket *packet =
			    enet_packet_create(packet_buffer.data(), (u32)packet_buffer.length(), ENET_PACKET_FLAG_RELIABLE);

			// Batch for sending
			enet_peer_send(m_sockets[peer_id]->m_peer, 0, packet);
			// Put the client in the right in_game state
			m_sockets[peer_id]->m_shook_hands = true;
		}
	}
}

void SlippiSpectateServer::SlippicommSocketThread(void)
{
	if (enet_initialize() != 0)
	{
		WARN_LOG(SLIPPI, "An error occurred while initializing spectator server.");
		return;
	}

	ENetAddress server_address = {0};
	server_address.host = ENET_HOST_ANY;
	server_address.port = SConfig::GetInstance().m_spectator_local_port;

	// Create the spectator server
	// This call can fail if the system is already listening on the specified port
	//  or for some period of time after it closes down. You basically have to just
	//  retry until the OS lets go of the port and we can claim it again
	//  This typically only takes a few seconds
	ENetHost *server = enet_host_create(&server_address, MAX_CLIENTS, 2, 0, 0);
	int tries = 0;
	while (server == nullptr && tries < 5)
	{
		WARN_LOG(SLIPPI, "Could not create spectator server on port %d", server_address.port);
		server = enet_host_create(&server_address, MAX_CLIENTS, 2, 0, 0);
		tries += 1;
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}

	if (server == nullptr)
	{
		WARN_LOG(SLIPPI, "Could not create spectator server on port %d", server_address.port);
		enet_deinitialize();
		return;
	}

	// The direct channel piggybacks on this thread for accepts only;
	// its per-event writes happen on the game thread (see directWrite).
	if (!SConfig::GetInstance().m_slippiDirectChannelPath.empty())
	{
		directListen(SConfig::GetInstance().m_slippiDirectChannelPath);
	}

	// Main slippicomm server loop
	while (1)
	{
		// If we're told to stop, then quit
		if (m_stop_socket_thread)
		{
#ifndef _WIN32
			int direct = m_direct_fd.exchange(-1);
			if (direct >= 0)
				close(direct);
			if (m_direct_listen_fd >= 0)
				close(m_direct_listen_fd);
#endif
			enet_host_destroy(server);
			enet_deinitialize();
			return;
		}

		directAccept();

		// Pop off any events in the queue
		popEvents();

		std::map<u16, std::shared_ptr<SlippiSocket>>::iterator it = m_sockets.begin();
		for (; it != m_sockets.end(); it++)
		{
			if (it->second->m_shook_hands)
			{
				writeEvents(it->first);
			}
		}

		// Non-blocking: flush the sends batched above and drain any
		//  incoming traffic, but do the WAITING on the condition variable
		//  below instead — write() wakes us the moment the game thread
		//  produces an event, so a frame's payload leaves on the same
		//  loop iteration instead of one-to-two service timeouts later
		//  (measured: the old loop paced spectators at a hard ~2.1ms per
		//  blocking-input frame, exactly two ~1ms service periods).
		ENetEvent event;
		while (enet_host_service(server, &event, 0) > 0)
		{
			switch (event.type)
			{
			case ENET_EVENT_TYPE_CONNECT:
			{

				INFO_LOG(SLIPPI, "A new spectator connected from %x:%u.\n", event.peer->address.host,
				         event.peer->address.port);

				std::shared_ptr<SlippiSocket> newSlippiSocket(new SlippiSocket());
				newSlippiSocket->m_peer = event.peer;
				m_sockets[event.peer->incomingPeerID] = newSlippiSocket;
				break;
			}
			case ENET_EVENT_TYPE_RECEIVE:
			{
				handleMessage(event.packet->data, (u32)event.packet->dataLength, event.peer->incomingPeerID);
				/* Clean up the packet now that we're done using it. */
				enet_packet_destroy(event.packet);

				break;
			}
			case ENET_EVENT_TYPE_DISCONNECT:
			{
				INFO_LOG(SLIPPI, "A spectator disconnected from %x:%u.\n", event.peer->address.host,
				         event.peer->address.port);

				// Delete the item in the m_sockets map
				m_sockets.erase(event.peer->incomingPeerID);
				/* Reset the peer's client information. */
				event.peer->data = NULL;
				break;
			}
			default:
			{
				INFO_LOG(SLIPPI, "Spectator sent an unknown ENet event type");
				break;
			}
			}
		}

		// Sleep until the game thread pushes an event (or 1ms, so enet
		//  keepalives/acks and incoming connects are still serviced at
		//  the old cadence when the game is idle). Re-checking Empty()
		//  under the lock bounds any lost wakeup at one timeout.
		{
			std::unique_lock<std::mutex> lock(m_wake_mutex);
			if (m_event_queue.Empty() && !m_stop_socket_thread)
			{
				m_wake_cv.wait_for(lock, std::chrono::milliseconds(1));
			}
		}
	}

	enet_host_destroy(server);
	enet_deinitialize();
}
