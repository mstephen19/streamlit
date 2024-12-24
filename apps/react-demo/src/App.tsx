import { pubSubHooks } from 'streamlit-react';
import './App.css';
import { useMemo, useState } from 'react';

const { usePublish, useSubscribe } = pubSubHooks<{
    chats: 'message';
}>({
    baseUrl: 'https://localhost:8000',
    path: '/events',
    credentials: 'include',
});

function Join({ onSubmit }: { onSubmit: (data: { roomName: string; nickname: string }) => void }) {
    const [roomName, setRoomName] = useState('');
    const [nickname, setNickname] = useState('');

    return (
        <form
            style={{
                display: 'flex',
                flexDirection: 'column',
                gap: '10px',
                width: '100%',
            }}
            onSubmit={(e) => {
                e.preventDefault();
                onSubmit({ roomName, nickname });
            }}>
            <input type='text' placeholder='Room Name' onChange={(e) => setRoomName(e.target.value.trim())} value={roomName} />
            <input type='text' placeholder='Your Nickname' onChange={(e) => setNickname(e.target.value.trim())} value={nickname} />

            <button type='submit' disabled={!roomName || !nickname}>
                Join
            </button>
        </form>
    );
}

function Chat({ roomName, nickname, onLeave }: { roomName: string; nickname: string; onLeave: () => void }) {
    const [message, setMessage] = useState('');
    const [messages, setMessages] = useState<{ sender: string; content: string; timestamp: number }[]>([]);

    const publish = usePublish({
        namespace: 'chats',
        key: roomName,
    });

    const queryParams = useMemo(() => ({ nickname }), [nickname]);

    const { connected, error } = useSubscribe({
        namespace: 'chats',
        key: roomName,
        eventName: 'message',
        eventHandler: (data) => {
            setMessages((prev) => [
                ...prev,
                { ...JSON.parse(data), timestamp: Date.now() } as { sender: string; content: string; timestamp: number },
            ]);
        },
        query: queryParams,
    });

    const handlePublish = async () => {
        setMessage('');
        await publish(
            'message',
            JSON.stringify({
                content: message,
            })
        );
    };

    if (error) {
        return <div>An error has occurred.</div>;
    }

    if (!connected) {
        return <div>Joining...</div>;
    }

    return (
        <div
            style={{
                height: '100%',
                width: '100%',
                display: 'flex',
                flexDirection: 'column',
                gap: '10px',
            }}>
            <button type='button' onClick={onLeave}>
                Leave
            </button>

            <div
                style={{
                    width: '100%',
                    flex: 1,
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '10px',
                    overflowY: 'scroll',
                }}>
                {messages.map(({ sender, content, timestamp }) => (
                    <div
                        key={`${sender}-${timestamp}`}
                        style={{
                            alignSelf: sender === nickname ? 'flex-end' : 'flex-start',
                            background: sender === nickname ? 'grey' : 'black',
                            display: 'flex',
                            gap: '5px',
                            maxWidth: '45%',
                            padding: '10px',
                            borderRadius: '25px',
                        }}>
                        <p
                            style={{
                                fontWeight: 'bold',
                            }}>
                            {sender}:
                        </p>
                        <p
                            style={{
                                overflowWrap: 'break-word',
                                wordBreak: 'break-all',
                            }}>
                            {content}
                        </p>
                    </div>
                ))}
            </div>

            <form
                style={{
                    display: 'flex',
                    width: '100%',
                    gap: '10px',
                }}>
                <textarea style={{ flex: 1 }} onChange={(e) => setMessage(e.target.value)} value={message}></textarea>

                <button type='submit' onClick={handlePublish} disabled={!message}>
                    Send
                </button>
            </form>
        </div>
    );
}

function App() {
    const [chatData, setChatData] = useState<{ roomName: string; nickname: string } | null>(null);

    return (
        <div
            style={{
                padding: '10px',
                height: '100%',
                width: '100%',
            }}>
            {!chatData && <Join onSubmit={setChatData} />}

            {Boolean(chatData) && <Chat onLeave={() => setChatData(null)} {...chatData!} />}
        </div>
    );
}

export default App;
